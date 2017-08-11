package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class FindDocuments {

  private static final String PRIMARY_TABLE = "/mapr/cluster1/apps/user_profile";
  private static final String SECONDARY_TABLE = "/mapr/cluster2/apps/user_profile";

  // Create an OJAI connection to MapR cluster
  private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

  public static void main(String[] args) throws IOException, InterruptedException {

    Logger log = LoggerFactory.getLogger(FindDocuments.class);

    // Create an "Enhanced" data store that support fail over to other cluster
    EnhancedJSONTable jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, SECONDARY_TABLE);


    // Build an OJAI query with an order by, offset, and limit
    final Query query = connection.newQuery()
            .orderBy("_id")
            .offset(5)
            .limit(5)
            .build();


    //infinite loop
    boolean loop = true;
    int counter = 0;

    while(loop) {
      final DocumentStream stream = jsonTable.findQuery(query);
      for (final Document doc : stream) {
        // Print the OJAI Document
        System.out.println(doc.asJsonString());
      }
      System.out.println(" == "+ counter++  +" == ");
      Thread.sleep(1000);
    }



    jsonTable.close();

  }

}
