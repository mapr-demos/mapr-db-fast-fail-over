package com.mapr.db;

//import com.mapr.db.store.EnhancedHTable;
//import com.mapr.org.apache.hadoop.hbase.util.Bytes;
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.hbase.HBaseConfiguration;
//import org.apache.hadoop.hbase.client.HTable;
//import org.apache.hadoop.hbase.client.Put;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public class Sample {

    private static final Logger log = LoggerFactory.getLogger(EnhancedBinaryTable.class);

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        System.out.println("\n=== START ===");

//        Configuration configuration = HBaseConfiguration.create();
//        Configuration configuration = HBaseConfiguration.create();
//
//        Put put = new Put(Bytes.toBytes("bgun"));
//        put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("first_name"), Bytes.toBytes("Bill"));
//        put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("flast_name"), Bytes.toBytes("Gun"));
//        put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("age"), Bytes.toBytes(37L));
//        String emailList = "{\"type\" : \"work\", \"email\" : \"bgun@mapr.com\"},{\"type\" : \"home\",\"email\" : \"bgun@mac.com\"}";
//        put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("emails"), Bytes.toBytes(emailList));
//
//        hTable.put(put);
//        hTable.close();
//        RetryPolicy policy = new RetryPolicy.Builder("/", "/alt")
//                .setTimeout(0)
//                .build();
//        System.out.println("\n=== END ===");
    }
}