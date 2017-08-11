/**
 * Copyright (c) 2017 MapR, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;

import java.io.IOException;

public class OJAI_006_FindQueryWithSelect {

  public static void main(final String[] args) throws IOException {

    System.out.println("==== Start Application ===");


    // Create an OJAI connection to MapR cluster
    final Connection connection = DriverManager.getConnection("ojai:mapr:");

    String primaryTable = "/demo_table";
    String secondaryTable = "/mapr/cluster2/demo_table";
    EnhancedJSONTable store = new EnhancedJSONTable(primaryTable, secondaryTable );

    // Build an OJAI query
    final Query query = connection.newQuery()
        .select("_id", "address.zipCode")
        .build();

    // fetch all OJAI Documents from this store
    final DocumentStream stream = store.findQuery(query);

    for (final Document userDocument : stream) {
      // Print the OJAI Document
      System.out.println(userDocument.asJsonString());      
    }

    // Close this instance of OJAI DocumentStore
    store.close();

    // close the OJAI connection and release any resources held by the connection
    connection.close();

    System.out.println("==== End Application ===");
  }

}