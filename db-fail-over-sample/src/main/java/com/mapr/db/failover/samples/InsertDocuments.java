package com.mapr.db.failover.samples;


import com.mapr.db.EnhancedJSONTable;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.io.IOException;
import java.util.UUID;


/**
 * Simple application that inserts a random document every second
 *
 */
public class InsertDocuments {


  private static final String PRIMARY_TABLE = "/mapr/cluster1/apps/user_profile";
  private static final String SECONDARY_TABLE = "/mapr/cluster2/apps/user_profile";

  // Create an OJAI connection to MapR cluster
  private static final Connection connection = DriverManager.getConnection("ojai:mapr:");


  public static void main(String[] args) throws IOException {


    Logger log = LoggerFactory.getLogger(InsertDocuments.class);

    // Create an "Enhanced" data store that support fail over to other cluster
    EnhancedJSONTable jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, SECONDARY_TABLE, 500);


    // Infinite loop to test insert
    boolean loop = true;
    while (loop) {

      Document doc = generateDocument();
      log.info("Inserting document "+ doc.getId());
      jsonTable.insert(doc);

      // sleep for 1sec
      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }


    }

    jsonTable.close();



  }


  /**
   * Create a "random" document
   * @return a new document with a ID generated from a UUID
   */
  private static Document generateDocument() {
      String newDocUUID = UUID.randomUUID().toString();
      return connection
              .newDocument()
              .setId("sample-" + newDocUUID)
              .set("name", "sample-" + newDocUUID);
  }


}
