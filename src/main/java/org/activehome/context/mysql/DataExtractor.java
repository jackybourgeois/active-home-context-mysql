package org.activehome.context.mysql;

/*
 * #%L
 * Active Home :: Context :: MySQL
 * $Id:$
 * $HeadURL:$
 * %%
 * Copyright (C) 2016 Active Home Project
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


import org.activehome.context.com.ContextRequest;
import org.activehome.context.com.ContextResponse;
import org.activehome.context.data.MetricRecord;
import org.activehome.context.data.Schedule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

/**
 * @author Jacky Bourgeois
 * @version %I%, %G%
 */
public class DataExtractor {

    private MySQLContext context;

    public DataExtractor(MySQLContext context) {
        this.context = context;
    }

    /**
     * Extract DataPoints from the context.
     *
     * @param contextReq The details of the data to extract
     * @param iteration  How many request should be performed
     * @param shift      Length of the time shift between each request
     */
    public ContextResponse extractSampleData(
            final ContextRequest contextReq,
            final Connection dbConnect,
            final int iteration,
            final long shift) throws SQLException {
        TreeMap<Integer, Schedule> scheduleMap = new TreeMap<>();
        // repeat request and shift startTS
        for (int i = 0; i < iteration; i++) {
            long start = contextReq.getStartTS() + i * shift;
            long end = contextReq.getStartTS() + i * shift + contextReq.getSampleDuration();
            Schedule schedule = new Schedule("schedule.extracted", start, end - start, contextReq.getFrequency());
            // for each requested metric
            for (String metricId : contextReq.getMetrics()) {
                long timeFrame = contextReq.getFrequency() * contextReq.getNumIteration()
                        - contextReq.getFrequency() + contextReq.getSampleDuration();
                MetricRecord mr = new MetricRecord(metricId, timeFrame);
                mr.setRecording(false);
                // for each sample
                for (int j = 0; j < contextReq.getNumIteration(); j++) {
                    long it = j * contextReq.getFrequency();
                    long sampleStart = start + it;
                    long sampleEnd = end + it;
                    ResultSet result = extractSampleData(dbConnect,
                            metricId, null, sampleStart, sampleEnd);
                    if (contextReq.isDiscrete()) {
                        addDiscreteSample(mr, result, sampleStart, sampleEnd);
                    } else {
                        addContinuousSample(mr, result, sampleStart, sampleEnd);
                    }
                }

                if (!contextReq.getFunction().equals("")) {
                    mr = applyFunctionOnSamples(contextReq.getFunction(), mr);
                }
                schedule.getMetricRecordMap().put(mr.getMetricId(), mr);
            }
            scheduleMap.put(i, schedule);
        }
        return new ContextResponse(contextReq, scheduleMap);
    }

    /**
     * Extract DataPoints from the context.
     */
    public Schedule extractData(final long startTS,
                                final long duration,
                                final long granularity,
                                final String[] metricArray,
                                final Connection dbConnect) throws SQLException {
        long endTS = startTS + duration;
        Schedule schedule = new Schedule("schedule.extractedData", startTS, duration, granularity);

        Set<String> metrics = new HashSet<>();
        for (String m : metricArray) {
            if (m.contains("*")) {
                metrics.addAll(getSetMetricId(m, dbConnect));
            } else {
                metrics.add(m);
            }
        }

        for (String metricId : metrics) {
            String[] metricVersionArray = metricId.split("#");
            MetricRecord mr = new MetricRecord(metricVersionArray[0], startTS, schedule.getHorizon());
            mr.setRecording(false);
            String[] versions = new String[]{"0"};
            if (metricVersionArray.length == 2) {
                versions = metricVersionArray[1].split(",");
            }

            metricId = metricVersionArray[0];

            ResultSet result = extractSampleData(dbConnect, metricId, versions, startTS, endTS);
            HashMap<String, String> initValMap = new HashMap<>();
            if (result!=null) {
                while (result.next()) {
                    long ts = result.getLong("time");
                    String val = result.getString("value");
                    long dpDuration = result.getLong("duration");
                    double confidence = result.getDouble("confidence");
                    String version = result.getString("version");
                    if (ts < startTS) {
                        initValMap.put(version, val);
                    } else {
                        if (mr.getRecords(version) == null
                                && initValMap.get(version) != null
                                && dpDuration == -1) {      // TODO manage discret data (with duration)
                            mr.addRecord(startTS, initValMap.get(version), version, confidence);
                        }
                        if (dpDuration != -1) {
                            mr.addRecord(ts, dpDuration, val, version, confidence);
                        } else {
                            mr.addRecord(ts, val, version, confidence);
                        }
                    }
                }
            }

            String mainVersion = null;
            for (String version : versions) {
                if (mainVersion == null && mr.getRecords(version) != null
                        && mr.getRecords(version).size() > 0) {
                    mr.setMainVersion(version);
                    mainVersion = version;
                }
            }

            schedule.getMetricRecordMap().put(mr.getMetricId(), mr);
        }

        return schedule;
    }

    private Set<String> getSetMetricId(final String metric,
                                       final Connection dbConnect) throws SQLException {

        String query = "SELECT `metricID` FROM `metrics`"
                + " WHERE `metricID` LIKE ?";

        String[] metricVersion = metric.split("#");
        if (metricVersion.length == 2) {
            String[] versions = metricVersion[1].split(",");
            if (versions.length > 0) {
                boolean first = true;
                for (String version : versions) {
                    if (first) {
                        query += " AND ( `version`=? ";
                        first = false;
                    } else {
                        query += " OR `version`=? ";
                    }
                }
                query += ")";
            }

        }
        query += " GROUP BY metricId";

        PreparedStatement prepStmt = dbConnect.prepareStatement(query);
        prepStmt.setString(1, metricVersion[0].replaceAll("\\*", "%"));
        if (metricVersion.length == 2) {
            String[] versions = metricVersion[1].split(",");
            for (int i = 0; i < versions.length; i++) {
                prepStmt.setString(i + 2, versions[i]);
            }
        }
        ResultSet result = prepStmt.executeQuery();
        Set<String> metrics = new HashSet<>();
        while (result.next()) {
            metrics.add(result.getString("metricId"));
        }
        return metrics;
    }

    private ResultSet extractSampleData(final Connection dbConnect,
                                        final String metricId,
                                        final String[] versions,
                                        final long startTS,
                                        final long endTS) {
        Long start = startTS / 1000;
        Long end = endTS / 1000;

        String query = "";
        LinkedList<Object> params = new LinkedList<>();
        boolean first = true;
        HashMap<String, Boolean> isDiscreteVersionMap =
                buildIsDiscreteVersionMap(dbConnect, metricId, versions);
        for (String version : versions) {
            if (isDiscreteVersionMap.get(version) != null) {
                if (!first) {
                    query += " UNION DISTINCT ";
                } else {
                    first = false;
                }
                if (!isDiscreteVersionMap.get(version)) {
                    query += "(SELECT \n" +
                            "    d.`value`,\n" +
                            "    d.`duration`,\n" +
                            "    UNIX_TIMESTAMP(d.`ts`) * 1000 AS 'ts',\n" +
                            "    UNIX_TIMESTAMP(d.`ts`) * 1000 AS 'time',\n" +
                            "    m.`version`,\n" +
                            "    d.`confidence`\n" +
                            "FROM `data` d JOIN `metrics` m ON d.`id` = m.`id`\n" +
                            "WHERE\n" +
                            "    m.`metricID` = ? \n" +
                            "        AND m.`version` = ? \n" +
                            "        AND UNIX_TIMESTAMP(d.`ts`) >= ? AND UNIX_TIMESTAMP(d.`ts`) < ? \n" +
                            "ORDER BY d.`ts`) UNION DISTINCT (SELECT \n" +
                            "    d.`value`,\n" +
                            "    d.`duration`,\n" +
                            "    UNIX_TIMESTAMP(d.`ts`) * 1000 AS 'ts',\n" +
                            "    UNIX_TIMESTAMP(d.`ts`) * 1000 AS 'time',\n" +
                            "    m.`version`,\n" +
                            "    d.`confidence`\n" +
                            "FROM `data` d JOIN `metrics` m ON d.`id` = m.`id`\n" +
                            "WHERE\n" +
                            "    m.`metricID` = ?\n" +
                            "        AND m.`version` = ?\n" +
                            "        AND UNIX_TIMESTAMP(d.`ts`) <= ?\n" +
                            "ORDER BY d.`ts` DESC\n" +
                            "LIMIT 1)";
                    Collections.addAll(params, metricId, version, start, end,
                            metricId, version, start);
                } else {
                    query += "(SELECT \n" +
                            "    d.`value`,\n" +
                            "    d.`duration`,\n" +
                            "    UNIX_TIMESTAMP(d.`ts`) * 1000 AS 'ts',\n" +
                            "    ((UNIX_TIMESTAMP(d.`ts`) * 1000) + m.`shift`) AS 'time',\n" +
                            "    m.`version`,\n" +
                            "    d.`confidence`\n" +
                            "FROM `data` d JOIN `metrics` m ON d.`id` = m.`id`\n" +
                            "WHERE\n" +
                            "    m.`metricID` = ?\n" +
                            "        AND m.`version` = ?\n" +
                            "        AND (UNIX_TIMESTAMP(d.`ts`) + m.`shift` / 1000) >= ? \n" +
                            "        AND (UNIX_TIMESTAMP(d.`ts`) + m.`shift` / 1000) < ? \n" +
                            "GROUP BY m.`shift`\n" +
                            "ORDER BY time DESC) UNION DISTINCT (SELECT \n" +
                            "    d.`value`,\n" +
                            "    d.`duration`,\n" +
                            "    UNIX_TIMESTAMP(d.`ts`) * 1000 AS 'ts',\n" +
                            "    ((UNIX_TIMESTAMP(d.`ts`) * 1000) + m.`shift`) AS 'time',\n" +
                            "    m.`version`,\n" +
                            "    d.`confidence`\n" +
                            "FROM `data` d JOIN `metrics` m ON d.`id` = m.`id`\n" +
                            "WHERE\n" +
                            "    m.`metricID` = ?\n" +
                            "        AND m.`version` = ?\n" +
                            "        AND (UNIX_TIMESTAMP(d.`ts`) + m.`shift` / 1000) <= ?\n" +
                            "GROUP BY m.`shift`\n" +
                            "ORDER BY time DESC\n" +
                            "LIMIT 1)";
                    Collections.addAll(params, metricId, version, start, end,
                            metricId, version, start);
                }
            }
        }
        query += " ORDER BY ts";
        PreparedStatement prepStmt = null;
        try {
            prepStmt = dbConnect.prepareStatement(query);
            for (int i = 0; i < params.size(); i++) {
                if (params.get(i) instanceof String) {
                    prepStmt.setString(i + 1, (String) params.get(i));
                } else if (params.get(i) instanceof Long) {
                    prepStmt.setLong(i + 1, (Long) params.get(i));
                }
            }
            return prepStmt.executeQuery();
        } catch (SQLException e) {
            System.out.println(prepStmt.toString());
            e.printStackTrace();
        }
        return null;
    }

    private HashMap<String, Boolean> buildIsDiscreteVersionMap(final Connection dbConnect,
                                                               final String metricId,
                                                               final String[] versions) {
        HashMap<String, Boolean> isDiscreteVersionMap = new HashMap<>();
        String query = "SELECT `metricID`, `version`, count(*) AS 'nbShifts'" +
                " FROM `metrics` WHERE `metricID`=?";
        for (String version : versions) {
            query += " OR `version`=?";
        }
        query += " GROUP BY `metricID`, `version`";
        PreparedStatement prepStmt = null;
        try {
            prepStmt = dbConnect.prepareStatement(query);
            prepStmt.setString(1, metricId);
            for (int i = 0; i < versions.length; i++) {
                prepStmt.setString(i + 2, versions[i]);
            }
            ResultSet result = prepStmt.executeQuery();
            while (result.next()) {
                isDiscreteVersionMap.put(result.getString("version"), result.getInt("nbShifts") > 1);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return isDiscreteVersionMap;
    }

    private void addDiscreteSample(final MetricRecord mr,
                                   final ResultSet result,
                                   final long start,
                                   final long end) throws SQLException {
        HashMap<String, Double> sum = new HashMap<>();
        HashMap<String, Double> sumConfidence = new HashMap<>();
        HashMap<String, Integer> index = new HashMap<>();
        HashMap<String, Long> prevTS = new HashMap<>();
        while (result.next()) {
            long ts = result.getLong("ts");
            double val = result.getDouble("value");
            String version = result.getString("version");
            if (!sum.containsKey(version)) {
                sum.put(version, 0.);
                sumConfidence.put(version, 0.);
                index.put(version, 0);
                prevTS.put(version, -1L);
            }
            sumConfidence.put(version, sumConfidence.get(version) + result.getDouble("confidence"));
            if (prevTS.get(version) != -1) {
                double toAdd;
                if (prevTS.get(version) < start) {
                    // necessary cast to get decimals (and not a rounded 0)
                    toAdd = (Double.valueOf(ts - start) / Double.valueOf(ts - prevTS.get(version))) * val;
                } else if (ts > end) {
                    toAdd = (Double.valueOf(end - prevTS.get(version)) / Double.valueOf(ts - prevTS.get(version))) * val;
                } else {
                    toAdd = val;
                }
                sum.put(version, sum.get(version) + toAdd);
            }
            prevTS.put(version, ts);
            index.put(version, index.get(version) + 1);
        }
        for (String version : sum.keySet()) {
            mr.addRecord(start, end - start, sum.get(version) + "", version,
                    sumConfidence.get(version) / index.get(version));
        }
    }

    private void addContinuousSample(final MetricRecord mr,
                                     final ResultSet result,
                                     final long start,
                                     final long end) throws SQLException {
        HashMap<String, Double> sum = new HashMap<>();
        HashMap<String, Double> sumConfidence = new HashMap<>();
        HashMap<String, Integer> index = new HashMap<>();
        HashMap<String, Long> prevTS = new HashMap<>();
        HashMap<String, Double> prevVal = new HashMap<>();
        while (result.next()) {
            long ts = result.getLong("ts");
            double val = result.getDouble("value");
            String version = result.getString("version");
            if (!sum.containsKey(version)) {
                sum.put(version, 0.);
                sumConfidence.put(version, 0.);
                index.put(version, 0);
                prevTS.put(version, -1L);
                prevVal.put(version, -1.);
            }
            sumConfidence.put(version, sumConfidence.get(version) + result.getDouble("confidence"));
            if (prevTS.get(version) != -1) {
                double toAdd;
                if (prevTS.get(version) < start) {
                    toAdd = (ts - start) * prevVal.get(version);
                } else if (ts > end) {
                    toAdd = (end - prevTS.get(version)) * prevVal.get(version);
                } else {
                    toAdd = (ts - prevTS.get(version)) * prevVal.get(version);
                }
                sum.put(version, sum.get(version) + toAdd);
            }
            prevTS.put(version, ts);
            prevVal.put(version, val);
            index.put(version, index.get(version) + 1);
        }
        for (String version : sum.keySet()) {
            mr.addRecord(start, end - start, (sum.get(version) / (end - start)) + "", version,
                    sumConfidence.get(version) / index.get(version));
        }
    }

    private MetricRecord applyFunctionOnSamples(final String function,
                                                final MetricRecord mr) {
        switch (function) {
            case "SUM":
                return ContextSamplingFunction.sum(mr);
            case "MAX":
                return ContextSamplingFunction.max(mr);
            default:
                return ContextSamplingFunction.avg(mr);
        }
    }

}
