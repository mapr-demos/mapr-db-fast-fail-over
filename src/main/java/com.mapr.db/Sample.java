package com.mapr.db;

import com.mapr.db.policy.RetryPolicy;
import com.mapr.db.store.EnhancedHTable;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

public class Sample {

  public static void main(String[] args) throws IOException {

    System.out.println("\n=== START ===");

    Configuration configuration = HBaseConfiguration.create();
    RetryPolicy policy = new RetryPolicy.Builder()
        .setNumOfReties(50)
        .setTimeout(1000)
        .setAlternateTable("/apps/table_2")
        .build();

    configuration.set("mapr.db.retry.policy", policy.toJson());
    HTable hTable = new EnhancedHTable(configuration, "/apps/table_1");


    Put put = new Put(Bytes.toBytes("bgun"));
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("first_name"), Bytes.toBytes("Bill"));
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("flast_name"), Bytes.toBytes("Gun"));
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("age"), Bytes.toBytes(37L));
    String emailList = "{\"type\" : \"work\", \"email\" : \"bgun@mapr.com\"},{\"type\" : \"home\",\"email\" : \"bgun@mac.com\"}";
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("emails"), Bytes.toBytes(emailList));

    hTable.put(put);
    hTable.close();
    System.out.println("\n=== END ===");
  }
}