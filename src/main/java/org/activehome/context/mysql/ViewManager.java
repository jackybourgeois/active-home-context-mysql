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
import org.activehome.com.RequestCallback;
import org.activehome.com.Status;
import org.activehome.com.error.*;
import org.activehome.com.error.Error;
import org.activehome.com.helper.JsonHelper;
import org.kevoree.log.Log;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Jacky Bourgeois
 * @version %I%, %G%
 */
public class ViewManager {

    /**
     * MySQL date parser.
     */
    private static SimpleDateFormat dfMySQL =
            new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    private MySQLContext context;

    public ViewManager(MySQLContext theContext) {
        context = theContext;
    }


//    public final Object saveWidgetView(final WidgetInstance widgetInstance,
//                                       final Connection dbConnect,
//                                       final Status status,
//                                       final long ts) throws SQLException {
//            String query = "INSERT IGNORE INTO widget_views "
//                    + " (id, userID, widgetID, x, y, width,"
//                    + " height, attributes, status, ts) "
//                    + " VALUES (?,?,?,?,?,?,?,?,?,?)";
//
//            PreparedStatement prepInsertStmt = dbConnect.prepareStatement(query);
//            prepInsertStmt.setString(1, widgetInstance.getId());
//            prepInsertStmt.setString(2, widgetInstance.getUser());
//            prepInsertStmt.setString(3, widgetInstance.getWidget().getComponent());
//            prepInsertStmt.setDouble(4, widgetInstance.getView().getPosX());
//            prepInsertStmt.setDouble(5, widgetInstance.getView().getPosY());
//            prepInsertStmt.setDouble(6, widgetInstance.getView().getWidth());
//            prepInsertStmt.setDouble(7, widgetInstance.getView().getHeight());
//            JsonObject attr = new JsonObject();
//            for (String key : widgetInstance.getAttributes().keySet()) {
//                attr.add(key, JsonHelper.objectToJson(
//                        widgetInstance.getAttributes().get(key)));
//            }
//            prepInsertStmt.setString(8, attr.toString());
//            prepInsertStmt.setString(9, status.name());
//            prepInsertStmt.setString(10, dfMySQL.format(ts));
//
//            prepInsertStmt.executeUpdate();
//            JsonObject result = new JsonObject();
//            result.add("saved", true);
//            return result;
//    }
//
//    public final WidgetInstance getWidgetView(final String widgetId,
//                                              final Connection dbConnect) throws SQLException {
//        String query = "SELECT * "
//                + " FROM widget_views v "
//                + " WHERE id=? "
//                + " ORDER BY v.ts DESC LIMIT 1";
//        PreparedStatement prepStmt = dbConnect.prepareStatement(query);
//        prepStmt.setString(1, widgetId);
//        ResultSet last = prepStmt.executeQuery();
//        if (last.next()) {
//            HashMap<String, Object> attr = new HashMap<>();
//            JsonObject jsonAttr = JsonObject.readFrom(
//                    last.getString("attributes")).asObject();
//            for (JsonObject.Member member : jsonAttr) {
//                attr.put(member.getName(), member.getValue().asString());
//            }
//            View view = new View(last.getInt("x"), last.getInt("y"),
//                    last.getDouble("width"), last.getDouble("height"));
//            Widget widget = new Widget(
//                    last.getString("widgetID"), "", "", "", new String[]{});
//            WidgetInstance wv = new WidgetInstance(widgetId,
//                    last.getString("userID"),
//                    Status.valueOf(last.getString("status")),
//                    attr, view, widget);
//        }
//        return null;
//    }
//
//    public final void getAllWidgetView(final String userID,
//                                       final Connection dbConnect,
//                                       final RequestCallback callback) throws SQLException {
//        ArrayList<WidgetInstance> widgetInstances = new ArrayList<>();
//        HashMap<String, String> requiredData = new HashMap<>();
//
//        String query = "SELECT v.id, widgetID, x, y, width, height, "
//                + " status, v.attributes, w.title, w.desc, w.import, w.img, w.data"
//                + " FROM widget_views v  JOIN widgets w ON v.widgetID=w.id"
//                + " WHERE userID=? AND v.ts=("
//                + " SELECT v2.ts "
//                + " FROM widget_views v2 "
//                + " WHERE v.id=v2.id "
//                + "ORDER BY v2.ts DESC LIMIT 1)";
//
//        PreparedStatement prepStmt = dbConnect.prepareStatement(query);
//        prepStmt.setString(1, context.getNode() + "." + userID);
//
//        ResultSet result = prepStmt.executeQuery();
//
//        while (result.next()) {
//            if (result.getString("status").compareTo(Status.DELETED.name()) != 0) {
//                HashMap<String, Object> attr = new HashMap<>();
//                JsonObject jsonAttr = JsonObject.readFrom(
//                        result.getString("attributes")).asObject();
//                for (JsonObject.Member member : jsonAttr) {
//                    attr.put(member.getName(),
//                            JsonHelper.jsonToObject(member.getValue()));
//                }
//
//                View view = new View(result.getInt("x"), result.getInt("y"),
//                        result.getDouble("width"), result.getDouble("height"));
//                Widget widget = new Widget(result.getString("widgetID"),
//                        result.getString("title"),
//                        result.getString("desc"),
//                        result.getString("img"),
//                        result.getString("import").split(","));
//
//                WidgetInstance wv = new WidgetInstance(result.getString("id"),
//                        userID,
//                        Status.valueOf(result.getString("status")),
//                        attr,
//                        view,
//                        widget);
//
//                // collect all data to load
//                if (result.getString("data").compareTo("") != 0) {
//                    for (String dataName : result.getString("data").split(",")) {
//                        requiredData.put(dataName, "");
//                        wv.getData().put(dataName, "");
//                    }
//                }
//
//                widgetInstances.add(wv);
//            }
//        }
//
//        String[] metricArray = requiredData.keySet()
//                .toArray(new String[requiredData.size()]);
//
//        if (requiredData.size() > 0) {
//            context.getLastData(metricArray, new RequestCallback() {
//                @Override
//                public void success(final Object result) {
//                    if (result instanceof JsonObject) {
//                        JsonObject jsonData = (JsonObject) result;
//                        for (WidgetInstance wv : widgetInstances) {
//                            wv.getData().keySet().stream()
//                                    .filter(key -> jsonData.get(key) != null)
//                                    .forEach(key -> wv.getData().put(key, jsonData.get(key)));
//                        }
//                    }
//
//                    System.out.println("### ### ### ready to send the list of widget view");
//                    for (WidgetInstance widget : widgetInstances) {
//                        System.out.println(widget);
//                    }
//
//                    callback.success(widgetInstances.toArray(
//                            new WidgetInstance[widgetInstances.size()]));
//                }
//
//                @Override
//                public void error(final Error result) {
//                    callback.error(result);
//                }
//            });
//        } else {
//            callback.success(widgetInstances.toArray(
//                    new WidgetInstance[widgetInstances.size()]));
//        }
//    }


}
