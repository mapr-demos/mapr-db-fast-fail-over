package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DuplicateInsertDefault {

  private static final String PRIMARY_TABLE = "/mapr/cluster1/apps/user_profile";
  private static final String SECONDARY_TABLE = "/mapr/cluster2/apps/user_profile";

  // Create an OJAI connection to MapR cluster
  private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

  public static void main(String[] args) throws IOException, InterruptedException {

    Logger log = LoggerFactory.getLogger(DuplicateInsertDefault.class);

    DocumentStore jsonTable = connection.getStore(PRIMARY_TABLE);

    Document doc = connection.newDocument("{\"_id\" : \"sample-01\", \"name\" : \"sample-01\"}");

    //insert 1
    jsonTable.insert(doc);


    // insert 2
    jsonTable.insert(doc);



  }

}
