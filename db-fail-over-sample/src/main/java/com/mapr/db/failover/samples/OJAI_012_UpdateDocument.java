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
import org.ojai.store.DocumentMutation;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class OJAI_012_UpdateDocument {

    private static final Logger LOG =
            LoggerFactory.getLogger(OJAI_012_UpdateDocument.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    public static void main(String[] args) throws IOException {

        LOG.info("==== Start Application ===");

        // Create an OJAI connection to MapR cluster
        final Connection connection = DriverManager.getConnection("ojai:mapr:");

        EnhancedJSONTable store = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);

        String docId = "user0002";

        // Print the document before update
        LOG.info("\t" + store.findById(docId).getMap("address").toString());

        // Create a DocumentMutation to update the zipCode field
        DocumentMutation mutation = connection.newMutation()
                .set("address.zipCode", 95196L);

        LOG.info("\tUpdating document " + docId);

        // Update the Document with '_id' = "user0002"
        store.update(docId, mutation);

        // Print the document after update
        LOG.info("\t" + store.findById(docId).getMap("address").toString());


        // Close this instance of OJAI DocumentStore
        store.close();

        // close the OJAI connection and release any resources held by the connection
        connection.close();

        LOG.info("==== End Application ===");
    }
}
