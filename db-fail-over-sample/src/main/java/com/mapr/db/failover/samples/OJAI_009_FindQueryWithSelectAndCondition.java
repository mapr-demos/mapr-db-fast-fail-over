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
import org.ojai.store.DriverManager;
import org.ojai.store.Query;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class OJAI_009_FindQueryWithSelectAndCondition {

    private static final Logger LOG =
            LoggerFactory.getLogger(OJAI_009_FindQueryWithSelectAndCondition.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    public static void main(final String[] args) throws IOException {

        LOG.info("==== Start Application ===");

        // Create an OJAI connection to MapR cluster
        final Connection connection = DriverManager.getConnection("ojai:mapr:");

        EnhancedJSONTable store = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);

        // Build an OJAI query with QueryCondition
        final Query query = connection.newQuery()
                .select("name", "address.zipCode").select("age").select("phoneNumbers[0]")
                .where("{\"$eq\": {\"address.zipCode\": 95196}}")
                .build();

        // fetch all OJAI Documents from this store
        final DocumentStream stream = store.findQuery(query);

        for (final Document userDocument : stream) {
            // Print the OJAI Document
            LOG.info(userDocument.asJsonString());
        }

        // Close this instance of OJAI DocumentStore
        store.close();

        // close the OJAI connection and release any resources held by the connection
        connection.close();

        LOG.info("==== End Application ===");
    }
}
