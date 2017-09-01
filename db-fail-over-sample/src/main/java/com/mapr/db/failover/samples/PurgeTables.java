package com.mapr.db.failover.samples;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Delete all document in tables
 */
@SuppressWarnings("Duplicates")
public class PurgeTables {

    private static final Logger LOG =
            LoggerFactory.getLogger(PurgeTables.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    // Create an OJAI connection to MapR cluster
    private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

    public static void main(String[] args) {

        LOG.info("==== START APPLICATION ====");

        // Get an instance of OJAI
        DocumentStore storeMaster = connection.getStore(PRIMARY_TABLE);
        DocumentStore storeFailOver = connection.getStore(FAILOVER_TABLE);

        DocumentStream streamMasterDocs = storeMaster.find();
        for (Document userDocument : streamMasterDocs) {
            storeMaster.delete(userDocument.getId());
        }
        storeMaster.flush();

        DocumentStream streamFailOverDocs = storeFailOver.find();
        for (Document userDocument : streamFailOverDocs) {
            storeFailOver.delete(userDocument.getId());
        }
        storeFailOver.flush();

        LOG.info("==== STOP APPLICATION ====");
    }
}
