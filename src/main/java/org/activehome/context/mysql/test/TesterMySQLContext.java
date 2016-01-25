package org.activehome.context.mysql.test;

/*
 * #%L
 * Active Home :: Context :: MySQL
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2016 org.activehome
 * %%
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as
 * published by the Free Software Foundation, either version 3 of the 
 * License, or (at your option) any later version.
 * 
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public 
 * License along with this program.  If not, see
 * <http://www.gnu.org/licenses/gpl-3.0.html>.
 * #L%
 */


import com.eclipsesource.json.JsonObject;
import org.activehome.com.Notif;
import org.activehome.com.Request;
import org.activehome.com.RequestCallback;
import org.activehome.com.error.*;
import org.activehome.com.error.Error;
import org.activehome.context.com.ContextRequest;
import org.activehome.context.com.ContextResponse;
import org.activehome.context.data.DataPoint;
import org.activehome.context.data.MetricRecord;
import org.activehome.context.data.Record;
import org.activehome.context.data.SampledRecord;
import org.activehome.context.data.Trigger;
import org.activehome.test.ComponentTester;
import org.activehome.time.TimeControlled;
import org.activehome.tools.Util;
import org.kevoree.annotation.ComponentType;
import org.kevoree.annotation.Input;
import org.kevoree.log.Log;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.LinkedList;
import java.util.Map;

/**
 * Mock component to test MySQLContext component.
 * - extractSampleData
 *
 * @author Jacky Bourgeois
 * @version %I%, %G%
 */
@ComponentType
public class TesterMySQLContext extends ComponentTester {

    /**
     * MySQL date parser.
     */
    private static SimpleDateFormat dfMySQL =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    /**
     * On start time, push a set of data to the context
     * and try to extract samples of these data.
     */
    @Override
    public final void onStartTime() {
        Trigger genTrigger = new Trigger("(^power\\.gen\\.)+(.*?)",
                "sum(power.gen.*)", "power.gen");
        Trigger consTrigger = new Trigger("^(power\\.import|power\\.export|power\\.gen)$",
                "${power.import,0}+${power.gen,0}-${power.export,0}", "power.cons");
        Trigger consEnergyTrigger = new Trigger("^power\\.cons$",
                "$-1{power.cons}*(($ts{power.cons}-$ts-1{power.cons})/3600000)", "energy.cons");
        Trigger genEnergyTrigger = new Trigger("^power\\.gen$",
                "$-1{power.gen}*(($ts{power.gen}-$ts-1{power.gen})/3600000)", "energy.gen");
        Trigger impEnergyTrigger = new Trigger("^power\\.import$",
                "$-1{power.import}*(($ts{power.import}-$ts-1{power.import})/3600000)", "energy.import");
        Trigger expEnergyTrigger = new Trigger("^power\\.export$",
                "$-1{power.export}*(($ts{power.export}-$ts-1{power.export})/3600000)", "energy.export");
        Trigger ctrlGenTrigger = new Trigger("^power\\.gen\\.solar",
                "(${time.dayTime,true}==true)?${triggerValue}:0", "");
        Trigger ctrlExportTrigger = new Trigger("^power\\.export",
                "(${triggerValue}<${power.gen,0}==true)?${triggerValue}:${power.gen,0}", "");

        Request triggerReq = new Request(getFullId(), getNode() + ".context",
                getCurrentTime(), "addTriggers",
                new Object[]{new Trigger[]{genTrigger, consTrigger, consEnergyTrigger, genEnergyTrigger,
                        impEnergyTrigger, expEnergyTrigger, ctrlGenTrigger, ctrlExportTrigger}});
        sendRequest(triggerReq, new RequestCallback() {
            @Override
            public void success(Object o) {
                sendTestData();
            }

            @Override
            public void error(Error error) {
                Log.error("Error while setting triggers: " + error);
            }
        });

    }

    @Override
    protected String logHeaders() {
        return null;
    }

    @Override
    protected JsonObject prepareNextTest() {
        return null;
    }

    private void sendTestData() {
        BufferedReader br = null;
        String line;
        try {
            InputStream is = this.getClass().getClassLoader().getResourceAsStream("test_data.csv");
            br = new BufferedReader(new InputStreamReader(is));
            LinkedList<DataPoint> dataPoints = new LinkedList<>();
            while ((line = br.readLine()) != null) {
                String[] dp = line.split(",");
                String metric;
                switch (dp[0]) {
                    case "Solarpv":
                        metric = "power.gen." + dp[0];
                        break;
                    case "import":
                        metric = "power." + dp[0];
                        break;
                    case "export":
                        metric = "power." + dp[0];
                        break;
                    default:
                        metric = "power.cons." + dp[0];
                }
                long ts = Long.valueOf(dp[2]) * 1000;
                dataPoints.add(new DataPoint(metric, ts, dp[1]));
            }
            sendNotif(new Notif(getFullId(), getNode() + ".context", getCurrentTime(),
                    dataPoints.toArray(new DataPoint[dataPoints.size()])));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (br != null) {
                try {
                    br.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * @param notifStr Notification from the context received as string
     */
    @Input
    public final void getNotif(final String notifStr) {
        Notif notif = new Notif(JsonObject.readFrom(notifStr));
        if (notif.getDest().compareTo(getFullId()) == 0
                && notif.getContent() instanceof String) {
            testContinuousSample();
        }
    }

    private void testContinuousSample() {
        try {
            long startSampleTest = dfMySQL.parse("2013-07-19 12:00:00").getTime();
            ContextRequest ctxAVGReq = new ContextRequest(new String[]{"power.gen.Solarpv"}, false,
                    startSampleTest, QUARTER, QUARTER, 1, "", "");
            Request extractAVGReq = new Request(getFullId(), getNode() + ".context", getCurrentTime(),
                    "extractSampleData", new Object[]{ctxAVGReq, 1, 0});
            sendRequest(extractAVGReq, new RequestCallback() {
                @Override
                public void success(final Object result) {
                    double val = ((ContextResponse) result).getResultMap().get(0)
                            .getMetricRecordMap().get(0).getRecords().get(0).getDouble();
                    boolean check = (Util.round5(val) == 2079.12333);
                    if (check) {
                        Log.info("Continuous sample: Done");
                        testDiscreteSample();
                    } else {
                        Log.error("Continuous sample: " + Util.round5(val) + ", should be " + 2079.12333);
                    }
                }

                @Override
                public void error(final Error error) {

                }
            });
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void testDiscreteSample() {
        try {
            long startSampleTest = dfMySQL.parse("2013-07-19 12:00:00").getTime();
            ContextRequest ctxAVGReq = new ContextRequest(new String[]{"energy.gen"}, true,
                    startSampleTest, QUARTER, QUARTER, 1, "", "");
            Request extractAVGReq = new Request(getFullId(), getNode() + ".context", getCurrentTime(),
                    "extractSampleData", new Object[]{ctxAVGReq, 1, 0});
            sendRequest(extractAVGReq, new RequestCallback() {
                @Override
                public void success(Object o) {
                    double val = ((ContextResponse) o).getResultMap().get(0)
                            .getMetricRecordMap().get(0).getRecords().get(0).getDouble();
                    boolean check = Util.round5(val) == 519.78083;
                    if (check) {
                        Log.info("Discrete sample: Done");
                        testSampleData();
                    } else {
                        Log.error("Discrete sample: " + Util.round5(val) + ", should be " + 519.78083);
                    }
                }

                @Override
                public void error(Error error) {

                }
            });
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void testSampleData() {
        try {
            long startTS = dfMySQL.parse("2013-07-19 00:00:00").getTime();
            ContextRequest ctxReq = new ContextRequest(new String[]{"energy.cons"}, true,
                    startTS, HOUR, HOUR, 24, "", "");
            Request extractReq = new Request(getFullId(), getNode() + ".context", getCurrentTime(),
                    "extractSampleData", new Object[]{ctxReq, 1, 0});
            sendRequest(extractReq, new RequestCallback() {
                @Override
                public void success(Object o) {
                    MetricRecord mr = ((ContextResponse) o).getResultMap().get(0).getMetricRecordMap().get(0);
                    int numSample = mr.getRecords().size();
                    double sampleDuration = (((SampledRecord)mr.getRecords().getFirst()).getDuration()*numSample)/24;
                    boolean check = Util.round5(numSample) == 24 && sampleDuration == HOUR;
                    if (check) {
                        Log.info("Right number of samples: Done");
                        Log.info("Right sample duration: Done");
                        testContinuousAVG();
                    } else {
                        Log.error("Number of sample: " + numSample + ", should be " + 24);
                        Log.error("Sample duration: " + sampleDuration + ", should be " + HOUR);
                    }
                }

                @Override
                public void error(Error error) {

                }
            });
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    private void testContinuousAVG() {
        try {
            long startTS = dfMySQL.parse("2013-07-19 12:00:00").getTime();
            ContextRequest ctxAVGReq = new ContextRequest(new String[]{"power.gen"}, false,
                    startTS, HOUR, HOUR, 4, "", "AVG");
            Request extractAVGReq = new Request(getFullId(), getNode() + ".context", getCurrentTime(),
                    "extractSampleData", new Object[]{ctxAVGReq, 1, 0});
            sendRequest(extractAVGReq, new RequestCallback() {
                @Override
                public void success(Object o) {
                    double val = ((ContextResponse) o).getResultMap().get(0)
                            .getMetricRecordMap().get(0).getRecords().get(0).getDouble();
                    boolean check = Util.round5(val) == 2260.82681;
                    if (check) {
                        Log.info("Discrete sample AVG: Done");
                    } else {
                        Log.error("Discrete sample AVG: " + Util.round5(val) + ", should be " + 2260.82681);
                    }
                }

                @Override
                public void error(Error error) {

                }
            });
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }


}
