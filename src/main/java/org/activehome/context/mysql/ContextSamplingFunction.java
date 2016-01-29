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


import org.activehome.context.data.MetricRecord;
import org.activehome.context.data.Record;
import org.activehome.context.data.SampledRecord;

/**
 * @author Jacky Bourgeois
 * @version %I%, %G%
 */
public class ContextSamplingFunction {

    public static MetricRecord avg(MetricRecord mr) {
        MetricRecord metricRecord = new MetricRecord(mr.getMetricId(), mr.getTimeFrame());
        for (String version : mr.getAllVersionRecords().keySet()) {
            double sum = 0;
            double confidence = 0;
            for (Record record : mr.getRecords()) {
                sum += record.getDouble();
                confidence += record.getConfidence();
            }
            metricRecord.addRecord(mr.getStartTime(),
                    ((SampledRecord)mr.getRecords().getFirst()).getDuration(),
                    sum / mr.getRecords().size() + "",
                    version,
                    confidence / mr.getRecords().size());
        }
        return metricRecord;
    }

    public static MetricRecord sum(MetricRecord mr) {
        MetricRecord metricRecord = new MetricRecord(mr.getMetricId(), mr.getTimeFrame());
        for (String version : mr.getAllVersionRecords().keySet()) {
            double sum = 0;
            double confidence = 0;
            for (Record record : mr.getRecords()) {
                sum += record.getDouble();
                confidence += record.getConfidence();
            }
            metricRecord.addRecord(mr.getStartTime(),
                    ((SampledRecord)mr.getRecords().getFirst()).getDuration(),
                    sum + "",
                    version,
                    confidence / mr.getRecords().size());
        }
        return metricRecord;
    }

    public static MetricRecord max(MetricRecord mr) {
        MetricRecord metricRecord = new MetricRecord(mr.getMetricId(), mr.getTimeFrame());
        for (String version : mr.getAllVersionRecords().keySet()) {
            double max = mr.getRecords().getFirst().getDouble();
            double confidence = mr.getRecords().getFirst().getConfidence();
            for (Record record : mr.getRecords()) {
                double val = record.getDouble();
                if (val > max) {
                    max = val;
                    confidence += record.getConfidence();
                }
            }
            metricRecord.addRecord(mr.getStartTime(),
                    ((SampledRecord)mr.getRecords().getFirst()).getDuration(),
                    max + "",
                    version,
                    confidence / mr.getRecords().size());
        }
        return metricRecord;
    }

}
