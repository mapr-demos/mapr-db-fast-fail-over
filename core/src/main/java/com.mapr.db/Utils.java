package com.mapr.db;

import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

public class Utils {

    private static final String DB_DRIVER_NAME = "ojai:mapr:";

    /**
     * Get DocumentStore from MapR-DB.
     * Table must exist.
     *
     * @param tableName Name that correspond to db table name
     * @return com.mapr.db.Table
     */
    public static DocumentStore getDocumentStore(String tableName) {
        Connection connection = DriverManager.getConnection(DB_DRIVER_NAME);
        return connection.getStore(tableName);
    }

}
