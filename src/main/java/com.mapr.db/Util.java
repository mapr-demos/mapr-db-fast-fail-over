package com.mapr.db;

import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class Util {

    private static final String DB_DRIVER_NAME = "ojai:mapr:";

    /**
     * This method check if the table exist,
     * and create Table object that used for manipulation with binary table.
     * <p>
     * If table doesn't exist it will create table in db.
     *
     * @param conn      Connection to the cluster
     * @param tableName Name that correspond to db table name
     * @return org.apache.hadoop.hbase.client.Table
     */
//    public static Table getBinaryTable(Connection conn, String tableName) {
//        try {
//            return getTableFromCluster(conn, tableName);
//        } catch (IOException e) {
//            throw new RuntimeException(e);
//        }
//    }

    /**
     * This method check if the table exist,
     * and create Table object that used for manipulation with Json table.
     * <p>
     * If table doesn't exist it will create table in db.
     *
     * @param clusterPath Path to the cluster
     * @param tableName   Name that correspond to db table name
     * @return com.mapr.db.Table
     */
    public static DocumentStore getJsonTable(String clusterPath, String tableName) {
        org.ojai.store.Connection connection =
                DriverManager.getConnection(DB_DRIVER_NAME + clusterPath);
        return connection.getStore(tableName);
    }

    /**
     * @param switched Field which need to switch after period of time
     */
    public static void createAndExecuteTaskForSwitchingTableBack(AtomicBoolean switched) {
        ScheduledExecutorService scheduler =
                Executors.newScheduledThreadPool(1);
        scheduler.schedule(() -> switched.set(false), 1000, TimeUnit.MILLISECONDS);
    }

//    private static Table getTableFromCluster(Connection conn, String tableName) throws IOException {
//        org.apache.hadoop.hbase.client.Admin admin = conn.getAdmin();
//
//        TableName tableNameValue = TableName.valueOf(tableName);
//
//        if (!admin.tableExists(tableNameValue)) {
//            admin.createTable(new HTableDescriptor(tableNameValue).addFamily(new HColumnDescriptor("default")));
//        }
//
//        return conn.getTable(tableNameValue);
//    }
}
