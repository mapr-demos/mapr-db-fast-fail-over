/**
 * Copyright (c) 2017 MapR, Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("Duplicates")
public class OJAI_013_ReadYourOwnWrite {

    private static final Logger LOG =
            LoggerFactory.getLogger(OJAI_013_ReadYourOwnWrite.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    public static void main(String[] args) {

        LOG.info("==== Start Application ===");


        // Create an OJAI connection to MapR cluster
        final Connection connectionNode1 = DriverManager.getConnection("ojai:mapr:");

        EnhancedJSONTable storeNode1 = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);


// TODO : Not yet implemented see issue #22
//    // initiate tracking of commit-context
//    storeNode1.beginTrackingWrites();
//
//    // issue a set of mutations/insert/delete/etc
//    storeNode1.update("user0000", connectionNode1.newMutation().set("address.zipCode", 95110L));
//    storeNode1.insertOrReplace(connectionNode1.newDocument(
//        "{\"_id\": \"user0004\", \"firstName\": \"Joel\", \"lastName\": \"Smith\", \"age\": 56, \"address\": {\"zipCode\":{\"$numberLong\":95110}}}"));
//
//    final String commitContext = storeNode1.endTrackingWrites();
//
//    // Close this instance of OJAI DocumentStore
//    storeNode1.close();

//    // close the OJAI connection and release any resources held by the connection
//    connectionNode1.close();
//
//    /*
//     * Next section of the code can run on the same or on a different node,
//     * the `commitContext` obtained earlier needs to be propagated to that node.
//     */
//
//    // Create an OJAI connection to MapR cluster
//    final Connection connectionNode2 = DriverManager.getConnection("ojai:mapr:");
//
//    // Get an instance of OJAI DocumentStore
//    final DocumentStore storeNode2 = connectionNode2.getStore("/demo_table");
//
//    // Build an OJAI query and set its commit context with timeout of 2 seconds
//    final Query query = connectionNode2.newQuery()
//        .select("_id", "firstName", "lastName", "address.zipCode")
//        .where("{\"$gt\": {\"address.zipCode\": 95110}}")
//        .waitForTrackedWrites(commitContext)
//        .build();
//
//    try {
//      // fetch all OJAI Documents from this store
//      final DocumentStream stream = storeNode2.findQuery(query);
//      for (final Document userDocument : stream) {
//        // Print the OJAI Document
//        LOG.info(userDocument.asJsonString());
//      }
//    } catch (QueryTimeoutException e) {
//      LOG.error("Timeout occurred while waiting for Query results");
//    }
//
//    // Close this instance of OJAI DocumentStore
//    storeNode2.close();
//
//    // close the OJAI connection and release any resources held by the connection
//    connectionNode2.close();

        LOG.info("==== End Application ===");
    }
}
