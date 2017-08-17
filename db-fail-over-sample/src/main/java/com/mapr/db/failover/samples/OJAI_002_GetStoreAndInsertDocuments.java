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
import com.mapr.ojai.examples.data.Dataset;
import com.mapr.ojai.examples.data.User;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.io.IOException;


@SuppressWarnings("Duplicates")
public class OJAI_002_GetStoreAndInsertDocuments {

  public static void main(String[] args) throws IOException {

    System.out.println("==== Start Application ===");

    // Create an OJAI connection to MapR cluster
    final Connection connection = DriverManager.getConnection("ojai:mapr:");

    String primaryTable = "/demo_table";
    String secondaryTable = "/mapr/cluster2/demo_table";
    EnhancedJSONTable store = new EnhancedJSONTable(primaryTable, secondaryTable );

    for (final User someUser : Dataset.users) {
      // Create an OJAI Document form the Java bean (there are other ways too)
      final Document userDocument = connection.newDocument(someUser);

      System.out.println("\t inserting "+ userDocument.getId());

      // insert the OJAI Document into the DocumentStore
      store.insertOrReplace(userDocument);
    }

    // Close this instance of OJAI DocumentStore
    store.close();

    // close the OJAI connection and release any resources held by the connection
    connection.close();

    System.out.println("==== End Application ===");
  }

}
