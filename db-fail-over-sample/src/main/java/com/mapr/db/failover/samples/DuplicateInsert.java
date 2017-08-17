package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.ojai.store.exceptions.DocumentExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DuplicateInsert {

  private static final String PRIMARY_TABLE = "/mapr/cluster1/apps/user_profile";
  private static final String SECONDARY_TABLE = "/mapr/cluster2/apps/user_profile";

  // Create an OJAI connection to MapR cluster
  private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

  public static void main(String[] args) throws IOException, InterruptedException {

    System.out.println(" ==== Start Application ===");

    Logger log = LoggerFactory.getLogger(DuplicateInsert.class);

    // Create an "Enhanced" data store that support fail over to other cluster
    EnhancedJSONTable jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, SECONDARY_TABLE);

    Document doc = connection.newDocument("{\"_id\" : \"sample-01\", \"name\" : \"sample-01\"}");

    log.info("Deleting sample ");
    jsonTable.delete("sample-01");
    // jsonTable.flush(); // TODO : Solved with issue #23

    //insert 1
    log.info("Inserting document a first time ");
    jsonTable.insert(doc);


    try {
      // insert 2
      log.info("Inserting document a second time, should fail ");
      jsonTable.insert(doc);
    } catch (DocumentExistsException e) {
      log.error("Cannot insert : document exists - {}", e.getMessage());
    }

    jsonTable.close();

    System.out.println(" ==== Stop Application ===");


  }

}
