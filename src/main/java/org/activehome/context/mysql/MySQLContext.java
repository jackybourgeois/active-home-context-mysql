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


import com.eclipsesource.json.JsonObject;
import org.activehome.com.Notif;
import org.activehome.com.RequestCallback;
import org.activehome.com.Status;
import org.activehome.com.error.Error;
import org.activehome.com.error.ErrorType;
import org.activehome.context.Context;
import org.activehome.context.com.ContextRequest;
import org.activehome.context.com.ContextResponse;
import org.activehome.context.data.Schedule;
import org.activehome.context.data.DataPoint;
import org.activehome.context.data.DiscreteDataPoint;
import org.activehome.context.data.MetricRecord;
import org.activehome.mysql.HelperMySQL;
import org.activehome.tools.Util;
import org.activehome.context.data.UserInfo;
import org.kevoree.annotation.ComponentType;
import org.kevoree.annotation.Param;
import org.kevoree.annotation.Stop;

import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;

/**
 * @author Jacky Bourgeois
 * @version %I%, %G%
 */
@ComponentType
public class MySQLContext extends Context {

    @Param(defaultValue = "MySQL implementation of Active Home context.")
    private String description;
    @Param(defaultValue = "/active-home-context-mysql")
    private String src;

    /**
     * MySQL date parser.
     */
    private static SimpleDateFormat dfMySQL =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    /**
     * MySQL connection
     */
    private Connection dbConnect;
    /**
     * Url of the data storage.
     */
    @Param(optional = false)
    private String urlSQLSource;
    @Param(defaultValue = "true")
    private boolean dropAndCreate;

    private DataExtractor dataExtractor;
    private ViewManager viewManager;

    @Override
    public final void start() {
        super.start();
        resetDB();
        dataExtractor = new DataExtractor(this);
        viewManager = new ViewManager(this);
    }

    @Override
    public void onInit() {
        super.onInit();
        resetDB();
        sendNotif(new Notif(getFullId(), "*", getCurrentTime(), Status.READY.name()));
    }

    private void resetDB() {
        if (dropAndCreate) {
            dropTable();
        }
        createTable();
    }

    @Stop
    public final void stop() {
        closeDbConnection(dbConnect);
    }

    /**
     * Connect to the MySQL db.
     *
     * @return the opened connection
     */
    private Connection openDbConnect() {
        return HelperMySQL.connect(urlSQLSource);
    }

    /**
     * Close the connection.
     *
     * @param connection The connection to close
     */
    private void closeDbConnection(final Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /**
     * Create a MySQL table to store the data.
     *
     * @return the full name of the created table
     */
    private boolean createTable() {
        PreparedStatement prepStmt = null;
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            String query = "CREATE TABLE IF NOT EXISTS `data` ("
                    + "  `id` INTEGER NOT NULL,"
                    + "  `ts` TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,"
                    + "  `value` TEXT NOT NULL,"
                    + "  `duration` LONG NOT NULL,"
                    + "  `confidence` DOUBLE DEFAULT 1,"
                    + " PRIMARY KEY (`id`,`ts`)) "
                    + " ENGINE=InnoDB DEFAULT CHARSET=latin1;";
            prepStmt = dbConnect.prepareStatement(query);
            prepStmt.executeUpdate();
            closeStatement(prepStmt);
            query = "CREATE TABLE IF NOT EXISTS `metrics` ("
                    + "  `id` INTEGER NOT NULL AUTO_INCREMENT,"
                    + "  `metricID` VARCHAR(100) NOT NULL,"
                    + "  `version` VARCHAR(100) DEFAULT '0',"
                    + "  `shift` INTEGER DEFAULT 0,"
                    + " PRIMARY KEY (`id`)) "
                    + " ENGINE=InnoDB DEFAULT CHARSET=latin1;";
            prepStmt = dbConnect.prepareStatement(query);
            prepStmt.executeUpdate();
            return true;
        } catch (SQLException exception) {
            logError("Create context table error: " + exception.getMessage());
            return false;
        } finally {
            closeStatement(prepStmt);
        }
    }

    /**
     * Drop the older MySQL table.
     */
    private boolean dropTable() {
        PreparedStatement prepStmt = null;
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            String query = "DROP TABLE IF EXISTS `data`";
            dbConnect.prepareStatement(query).executeUpdate();
            query = "DROP TABLE IF EXISTS `metrics`;";
            prepStmt = dbConnect.prepareStatement(query);
            prepStmt.executeUpdate();
            return true;
        } catch (SQLException exception) {
            logError("Drop context table error: " + exception.getMessage());
            return false;
        } finally {
            closeStatement(prepStmt);
        }
    }

    @Override
    public final void save(final DataPoint dp) {
        PreparedStatement prepStmt = null;
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }

            long duration = -1;
            if (dp instanceof DiscreteDataPoint) {
                duration = ((DiscreteDataPoint) dp).getInterval();
            }

            String query = "INSERT INTO `data`"
                    + " (`id`,`ts`,`value`,`duration`,`confidence`) VALUES (?,?,?,?,?)"
                    + " ON DUPLICATE KEY UPDATE `value`=?, `duration`=?, `confidence`=?";
            prepStmt = dbConnect.prepareStatement(query);
            int id = getIdInsertIfNotExists(dp);
            if (id != -1) {
                prepStmt.setInt(1, id);
                prepStmt.setString(2, dfMySQL.format(dp.getTS()));
                prepStmt.setString(3, dp.getValue());
                prepStmt.setLong(4, duration);
                prepStmt.setDouble(4, dp.getConfidence());
                prepStmt.setString(5, dp.getValue());
                prepStmt.setLong(6, duration);
                prepStmt.setDouble(7, dp.getConfidence());
                prepStmt.executeUpdate();
            } else {
                logError("Save: wrong id for metric " + dp.getMetricId());
            }
        } catch (SQLException exception) {
            logError("save data point: " + exception.getMessage());
        } finally {
            closeStatement(prepStmt);
        }
    }

    private void closeStatement(PreparedStatement prepStmt) {
        if (prepStmt != null) {
            try {
                prepStmt.close();
            } catch (SQLException e) {
                logError("Closing statement: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void closeResultSet(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException e) {
                logError("Closing resultSet: " + e.getMessage());
            }
        }
    }

    private int getIdInsertIfNotExists(final DataPoint dp) {
        int id = getId(dp);
        if (id != -1) {
            return id;
        }
        PreparedStatement prepStmt = null;
        ResultSet tableKeys = null;
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            String insertQuery = "INSERT IGNORE INTO `metrics`"
                    + " (`metricID`,`version`,`shift`) VALUES (?,?,?)";
            prepStmt = dbConnect.prepareStatement(insertQuery,
                    Statement.RETURN_GENERATED_KEYS);
            prepStmt.setString(1, dp.getMetricId());
            prepStmt.setString(2, dp.getVersion());
            prepStmt.setLong(3, dp.getShift());
            prepStmt.executeUpdate();
            tableKeys = prepStmt.getGeneratedKeys();
            tableKeys.next();
            id = tableKeys.getInt(1);
            closeResultSet(tableKeys);
            return id;
        } catch (SQLException exception) {
            logError("Insert metric error: " + exception.getMessage());
        } finally {
            closeStatement(prepStmt);

        }
        return -1;
    }

    private int getId(final DataPoint dp) {
        PreparedStatement prepStmt = null;
        ResultSet result = null;
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            String selectQuery = "SELECT id FROM `metrics`"
                    + " WHERE `metricID`=? AND `version`=? AND `shift`=?";
            prepStmt = dbConnect.prepareStatement(selectQuery,
                    Statement.RETURN_GENERATED_KEYS);
            prepStmt.setString(1, dp.getMetricId());
            prepStmt.setString(2, dp.getVersion());
            prepStmt.setLong(3, dp.getShift());
            result = prepStmt.executeQuery();
            if (result.next()) {
                return result.getInt("id");
            }
        } catch (SQLException exception) {
            logError("Get metric id error while looking for " + dp.getMetricId() + ": " + exception.getMessage());
        } finally {
            closeStatement(prepStmt);
            closeResultSet(result);
        }
        return -1;
    }

    @Override
    public final void save(final List<DataPoint> dpList) {
        PreparedStatement prepStmt = null;
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            String query = "INSERT INTO `data`"
                    + " (`id`, `ts`, `value`, `duration`, `confidence`) VALUES (?, ?, ?, ?, ?)"
                    + " ON DUPLICATE KEY UPDATE `value`=?, `duration`=?, `confidence`=?";
            prepStmt = dbConnect.prepareStatement(query);
            int i = 0;
            dbConnect.setAutoCommit(false);
            for (DataPoint dp : dpList) {
                int id = getIdInsertIfNotExists(dp);

                long duration = -1;
                if (dp instanceof DiscreteDataPoint) duration = ((DiscreteDataPoint) dp).getInterval();

                if (id != -1) {
                    prepStmt.setInt(1, id);
                    prepStmt.setString(2, dfMySQL.format(dp.getTS()));
                    prepStmt.setString(3, dp.getValue());
                    prepStmt.setLong(4, duration);
                    prepStmt.setDouble(5, dp.getConfidence());
                    prepStmt.setString(6, dp.getValue());
                    prepStmt.setLong(7, duration);
                    prepStmt.setDouble(8, dp.getConfidence());
                    prepStmt.addBatch();
                    i++;
                    if (i % 1000 == 0 || i == dpList.size()) {
                        prepStmt.executeBatch(); // Execute every 1000 items.
                    }
                }
            }
            dbConnect.commit();
        } catch (SQLException exception) {
            logError("save list of data point: " + exception.getMessage());
            logError(String.valueOf(prepStmt));
        } finally {
            closeStatement(prepStmt);
        }

    }

    @Override
    public final void getLastData(final String[] metricArray,
                                  final RequestCallback callback) {
        JsonObject requestedData = new JsonObject();
        PreparedStatement prepStmt = null;
        ResultSet result = null;
        for (String metric : metricArray) {
            DataPoint dp = getCurrentDP(metric, "0", 0);
            if (dp != null) {
                requestedData.add(metric, dp.toJson());
            } else {
                String query = "SELECT d.`value`, d.`ts`, d.`duration`, d.`confidence`"
                        + " FROM `data` d JOIN `metrics` m ON d.`id`=m.`id`"
                        + " WHERE m.`metricID`=? && m.`version`='0' "
                        + " ORDER BY `ts` DESC LIMIT 1";
                try {
                    if (dbConnect == null || dbConnect.isClosed()) {
                        dbConnect = openDbConnect();
                    }
                    prepStmt = dbConnect.prepareStatement(query);
                    prepStmt.setString(1, metric);
                    result = prepStmt.executeQuery();
                    while (result.next()) {
                        String val = result.getString(metric);
                        long ts = result.getLong("ts");
                        long duration = result.getLong("duration");
                        if (val != null) {
                            if (duration != -1) {
                                requestedData.add(metric,
                                        new DiscreteDataPoint(metric, ts, val, duration).toJson());
                            } else {
                                requestedData.add(metric,
                                        new DataPoint(metric, ts, val).toJson());
                            }
                        }
                    }
                } catch (SQLException e) {
                    callback.error(new Error(ErrorType.METHOD_ERROR, e.getMessage()));
                } finally {
                    closeStatement(prepStmt);
                    closeResultSet(result);
                }
            }
        }
        callback.success(requestedData);
    }

    @Override
    public final void getData(final String path,
                              final long start,
                              final long end,
                              final RequestCallback callback) {
        HashMap<String, MetricRecord> metricRecordMap = new HashMap<>();
        PreparedStatement prepStmt = null;
        ResultSet result = null;
        try {
            String query = "SELECT m.`metricID`, d.`value`, d.`ts` "
                    + " FROM `data` d JOIN `metrics` m ON d.`id`=m.`id`"
                    + " WHERE m.`metricId` LIKE ? AND m.`version`='0'"
                    + "     AND d.`ts` BETWEEN ? AND ? "
                    + " ORDER BY d.`ts`";
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            prepStmt = dbConnect.prepareStatement(query);
            prepStmt.setString(1, path.replaceAll("\\*", "%"));
            prepStmt.setString(2, dfMySQL.format(start));
            prepStmt.setString(3, dfMySQL.format(end));
            result = prepStmt.executeQuery();
            while (result.next()) {
                String metricId = result.getString("metricID");
                long ts = result.getLong("ts");
                String val = result.getString("value");
                long duration = result.getLong("duration");
                double confidence = result.getDouble("confidence");
                if (!metricRecordMap.containsKey(metricId)) {
                    metricRecordMap.put(metricId,
                            new MetricRecord(metricId, end - start, "0", ts, val, confidence));
                } else {
                    if (duration != -1) {
                        metricRecordMap.get(metricId).addRecord(ts, duration, val, confidence);
                    } else {
                        metricRecordMap.get(metricId).addRecord(ts, val, confidence);
                    }
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            closeStatement(prepStmt);
            closeResultSet(result);
        }

        JsonObject json = new JsonObject();
        for (String metric : metricRecordMap.keySet()) {
            json.add(metric, metricRecordMap.get(metric).toJson());
        }
        callback.success(json);
    }

    @Override
    public final void extractSchedule(final long start,
                                      final long duration,
                                      final long granularity,
                                      final String[] metricArray,
                                      final RequestCallback callback) {
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            Schedule schedule = dataExtractor.extractData(
                    start, duration, granularity, metricArray, dbConnect);
            callback.success(schedule);
        } catch (SQLException e) {
            callback.error(new Error(ErrorType.METHOD_ERROR, "SQL exception: " + e.getMessage()));
        }
    }

    /**
     * Extract DataPoints from the context.
     *
     * @param contextReq The details of the data to extract
     * @param iteration  How many request should be performed
     * @param shift      Length of the time shift between each request
     * @param callback   The callback to reply
     */
    @Override
    public final void extractSampleData(final ContextRequest contextReq,
                                        final int iteration,
                                        final long shift,
                                        final RequestCallback callback) {
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            ContextResponse response =
                    dataExtractor.extractSampleData(contextReq, dbConnect, iteration, shift);
            callback.success(response);
        } catch (SQLException e) {
            callback.error(new Error(ErrorType.METHOD_ERROR, "SQL exception: " + e.getMessage()));
        }
    }

    @Override
    public final void getAllAvailableMetrics(final UserInfo userInfo,
                                             final RequestCallback callback) {
        PreparedStatement prepStmt = null;
        ResultSet result = null;
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }

            String query = "SELECT m.`metricID`, m.`version`, m.`shift`, d.`value`, d.`ts`, "
                    + " d.`confidence`, d.`duration` "
                    + " FROM `data` d JOIN `metrics` m ON d.`id`=m.`id`"
                    + " GROUP BY m.`metricID`, m.`version`, m.`shift`, d.`value`, d.`ts`, d.`confidence`, d.`duration`"
                    + " ORDER BY d.`ts` DESC";

            prepStmt = dbConnect.prepareStatement(query);
            result = prepStmt.executeQuery();

            JsonObject allMetrics = new JsonObject();
            while (result.next()) {
                String metricId = result.getString("metricID");
                String version = result.getString("version");
                Long shift = result.getLong("shift");
                long ts = result.getLong("ts");
                String val = result.getString("value");
                long duration = result.getLong("duration");
                double confidence = result.getDouble("confidence");
                if (allMetrics.get(metricId)==null) {
                    allMetrics.add(metricId, new JsonObject());
                }
                JsonObject jMetric = allMetrics.get(metricId).asObject();
                if (jMetric.get(version)==null) {
                    jMetric.add(version, new JsonObject());
                }
                JsonObject jVersion = jMetric.get(version).asObject();
                if (duration==-1) {
                    jVersion.add(shift+"", new DataPoint(metricId, ts, val,
                            version, shift, confidence).toJson());
                } else {
                    jVersion.add(shift+"", new DiscreteDataPoint(metricId, ts, val,
                            version, shift, confidence, duration).toJson());
                }
            }

            callback.success(allMetrics);
        } catch (SQLException e) {
            callback.error(new Error(ErrorType.METHOD_ERROR, "SQL exception: " + e.getMessage()));
        } finally {
            closeStatement(prepStmt);
            closeResultSet(result);
        }
    }

    /*@Override
    public final void saveWidgetView(final WidgetInstance widgetInstance,
                                     final Status status,
                                     final long ts,
                                     RequestCallback callback) {
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            callback.success(viewManager.saveWidgetView(widgetInstance, dbConnect, status, ts));
        } catch (SQLException e) {
            e.printStackTrace();
            callback.error(new org.activehome.com.error.Error(ErrorType.SAVE_ERROR,
                    "Error to save WidgetView " + widgetInstance.getId()
                            + "(" + widgetInstance.getWidget().getComponent()
                            + ") - " + e.getMessage()));
        }
    }

    @Override
    public final void getWidgetView(final String widgetId,
                                    final RequestCallback callback) {
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            WidgetInstance wi = viewManager.getWidgetView(widgetId, dbConnect);
            if (wi != null) {
                callback.success(wi);
            } else {
                callback.error(new Error(ErrorType.NOT_FOUND, "Unknown widget id."));
            }
        } catch (SQLException e) {
            e.printStackTrace();
            callback.error(new Error(ErrorType.METHOD_ERROR, e.getMessage()));
        }
    }

    @Override
    public final void getAllWidgetView(final String userID,
                                       final RequestCallback callback) {
        try {
            if (dbConnect == null || dbConnect.isClosed()) {
                dbConnect = openDbConnect();
            }
            viewManager.getAllWidgetView(userID, dbConnect, callback);
        } catch (SQLException e) {
            e.printStackTrace();
            callback.error(new Error(ErrorType.METHOD_ERROR, e.getMessage()));
        }
    }*/

//    private MetricRecord loadMetricRecord(final String metric,
//                                          final long start,
//                                          final long end) {
//        MetricRecord metricRecord = new MetricRecord(metric, end - start);
//        String query = "SELECT * FROM data";
//        try {
//            if (dbConnect == null || dbConnect.isClosed()) {
//                dbConnect = openDbConnect();
//            }
//            PreparedStatement prepStmt;
//            query += " WHERE metricId=? AND ts BETWEEN ? AND ?"; // get range
//            prepStmt = dbConnect.prepareStatement(query);
//            prepStmt.setString(1, metric);
//            prepStmt.setString(2, dfMySQL.format(start));
//            prepStmt.setString(3, dfMySQL.format(end));
//            ResultSet result = prepStmt.executeQuery();
//            while (result.next()) {
//                metricRecord.addRecord(result.getLong("ts"), result.getString("value"));
//            }
//        } catch (SQLException e) {
//            e.printStackTrace();
//        }
//        return metricRecord;
//    }
//
//    public final String getTableName() {
//        return tableName;
//    }

}
