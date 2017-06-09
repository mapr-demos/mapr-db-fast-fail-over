package com.mapr.db;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.Table;

import java.io.IOException;

public class Util {
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
