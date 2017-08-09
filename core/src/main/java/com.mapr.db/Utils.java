package com.mapr.db;

import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

public class Utils {

    private static final String DB_DRIVER_NAME = "ojai:mapr:";

    /**
     * This method check if the table exist,
     * and create Table object that used for manipulation with Json table.
     * <p>
     * If table doesn't exist it will create table in db.
     *
     * @param tableName Name that correspond to db table name
     * @return com.mapr.db.Table
     */
    public static DocumentStore getJsonTable(String tableName) {
        org.ojai.store.Connection connection =
                DriverManager.getConnection(DB_DRIVER_NAME);
        return connection.getStore(tableName);
    }

}
