package com.mapr.db.failover.samples;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.QueryCondition;

/**
 *  Delete all document in tables
 */
public class PurgeTables {

  private static final String PRIMARY_TABLE = "/mapr/cluster1/apps/user_profile";
  private static final String SECONDARY_TABLE = "/mapr/cluster2/apps/user_profile";

  // Create an OJAI connection to MapR cluster
  private static final Connection connection = DriverManager.getConnection("ojai:mapr:");


  public static void main(String[] args) {


    System.out.println("==== START APPLICATION ====");

    // Get an instance of OJAI
    DocumentStore storeMaster = connection.getStore(PRIMARY_TABLE);
    DocumentStore storeFailOver = connection.getStore(SECONDARY_TABLE);

    DocumentStream streamMasterDocs = storeMaster.find();
    for (Document userDocument : streamMasterDocs) {
      storeMaster.delete( userDocument.getId() );
    }
    storeMaster.flush();


    DocumentStream streamFailOverDocs = storeFailOver.find();
    for (Document userDocument : streamFailOverDocs) {
      storeFailOver.delete( userDocument.getId() );
    }
    storeFailOver.flush();

    System.out.println("==== STOP APPLICATION ====");



  }

}
