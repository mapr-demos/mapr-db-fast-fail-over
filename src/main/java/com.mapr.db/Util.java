package com.mapr.db;

import lombok.experimental.UtilityClass;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

@UtilityClass
public class Util {

    /**
     * This method check if the table exist,
     * and create Table object that used for manipulation with table.
     * <p>
     * If table doesn't exist it will create table in db.
     *
     * @param tableName Name that correspond to db table name
     * @return Table
     * @throws IOException if will be some issue with db
     */
    public static Table getTable(String tableName) throws IOException {
        Configuration conf = HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        org.apache.hadoop.hbase.client.Admin admin = conn.getAdmin();

        TableName tableNameValue = TableName.valueOf(tableName);

        if (!admin.tableExists(tableNameValue)) {
            admin.createTable(new HTableDescriptor(tableNameValue).addFamily(new HColumnDescriptor("default")));
        }

        return conn.getTable(tableNameValue);
    }
}
