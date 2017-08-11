package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import com.sun.javadoc.Doc;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;

public class DuplicateInsert {

  private static final String PRIMARY_TABLE = "/mapr/cluster1/apps/user_profile";
  private static final String SECONDARY_TABLE = "/mapr/cluster2/apps/user_profile";

  // Create an OJAI connection to MapR cluster
  private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

  public static void main(String[] args) throws IOException, InterruptedException {

    Logger log = LoggerFactory.getLogger(DuplicateInsert.class);

    // Create an "Enhanced" data store that support fail over to other cluster
    EnhancedJSONTable jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, SECONDARY_TABLE);

    Document doc = connection.newDocument("{\"_id\" : \"sample-01\", \"name\" : \"sample-01\"}");

    //insert 1
    jsonTable.insert(doc);


    // insert 2
    jsonTable.insert(doc);



    jsonTable.close();

  }

}
