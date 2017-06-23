package com.mapr.db;

import com.mapr.db.policy.RetryPolicy;
import com.mapr.db.store.EnhancedHTable;
import com.mapr.org.apache.hadoop.hbase.util.Bytes;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;

@Slf4j
public class BaseTests {

  private Configuration configuration;
  private RetryPolicy policy;

  public BaseTests() {
    this.configuration = HBaseConfiguration.create();
    this.policy = new RetryPolicy.Builder()
        .setNumOfReties(50)
        .setTimeout(50000)
        .setAlternateSuffix(".foo")
        .setAlternateTable("/some/different/table")
        .build();
  }

  private static Put getPut() {
    Put put = new Put(Bytes.toBytes("jdoe"));
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("first_name"), Bytes.toBytes("John"));
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("flast_name"), Bytes.toBytes("Doe"));
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("age"), Bytes.toBytes(40L));
    String emailList = "{\"type\" : \"work\", \"email\" : \"jdoe@mapr.com\"},{\"type\" : \"home\",\"email\" : \"jdoe@mac.com\"}";
    put.addColumn(Bytes.toBytes("default"), Bytes.toBytes("emails"), Bytes.toBytes(emailList));
    return put;
  }

  @Test
  public void testRetryPolicyBuilder() throws IOException {
    RetryPolicy policy = new RetryPolicy.Builder()
        .setNumOfReties(50)
        .setTimeout(100)
        .setAlternateSuffix(".foo")
        .setAlternateTable("/some/different/table")
        .build();
    log.info(policy.toString());
  }

  @Test
  @Ignore
  public void testRemove() throws IOException {
    HTable hTable = new EnhancedHTable(configuration, "/apps/table_1", policy);
    hTable.put(getPut());
  }

}
