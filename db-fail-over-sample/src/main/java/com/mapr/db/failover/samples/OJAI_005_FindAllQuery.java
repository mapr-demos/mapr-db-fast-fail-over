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
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static java.lang.Thread.sleep;

@SuppressWarnings("Duplicates")
public class OJAI_005_FindAllQuery {

    private static final Logger LOG =
            LoggerFactory.getLogger(OJAI_005_FindAllQuery.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster3/apps/user_profiles";

    // Create an OJAI connection to MapR cluster
    private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

    public static void main(final String[] args) throws IOException, InterruptedException {

        LOG.info("==== Start Application ===");

        EnhancedJSONTable stores = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);

        DocumentStore primary = connection.getStore(FAILOVER_TABLE);

        // Build an OJAI query
        final Query query = connection.newQuery()
                .orderBy("_id")
                .offset(5)
                .limit(5)
                .build();

        int counter = 0;
        while(true) {
            final DocumentStream stream = stores.findQuery(query);

            try {
                for (final Document doc : stream) {
                    // Print the OJAI Document
                    LOG.info(doc.asJsonString());
                }

            } catch (StoreException se) {
                LOG.info(se.getMessage());

                DocumentStream replica = primary.findQuery(query);
                for (final Document doc : replica) {
                    // Print the OJAI Document
                    LOG.info(doc.asJsonString());
                }
                break;
            }
            LOG.info(" == "+ counter++  +" == ");
            sleep(1000);
        }

        stores.close();
        primary.close();

        LOG.info("==== End Application ===");
    }
}
