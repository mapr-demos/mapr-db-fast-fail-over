package com.mapr.db;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;

import java.util.UUID;

public class EnhancedJsonTableTest {

    private static final int NUMBER_OF_REQUESTS = 100;

    private static final String DRIVER_NAME = "ojai:mapr:";

    private static final String PRIMARY_CLUSTER = "cyber.mapr.cluster.C1";
    private static final String SECONDARY_CLUSTER = "cyber.mapr.cluster.C2";

    private static final String PRIMARY_TABLE = "/apps/primary";
    private static final String SECONDARY_TABLE = "/apps/secondary";

    private static final int TIMEOUT = 5;

    //    TODO: Find method to slow connection with primary cluster or shutdown it from test
    @Test
    @Ignore("Needs configuration data for the two reachable clusters")
    public void testInsert() {
        Connection primaryConn =
                DriverManager.getConnection(DRIVER_NAME + PRIMARY_CLUSTER);
        Connection secondaryConn =
                DriverManager.getConnection(DRIVER_NAME + SECONDARY_CLUSTER);

        DocumentStore primaryStore = primaryConn.getStore(PRIMARY_TABLE);
        DocumentStore secondaryStore = secondaryConn.getStore(SECONDARY_TABLE);

        EnhancedJSONTable jsonTable =
                new EnhancedJSONTable(PRIMARY_CLUSTER, PRIMARY_TABLE,
                        SECONDARY_CLUSTER, SECONDARY_TABLE, TIMEOUT);

        for (int i = 0; i < NUMBER_OF_REQUESTS; i++) {
            jsonTable.insert(getNewDocument(primaryConn));
        }

        Assert.assertEquals(0, countDocuments(primaryStore));
        Assert.assertEquals(NUMBER_OF_REQUESTS, countDocuments(secondaryStore));

        closeStoreAndConn(primaryConn, primaryStore);
        closeStoreAndConn(secondaryConn, secondaryStore);
    }


    private void closeStoreAndConn(Connection primaryConn, DocumentStore primaryStore) {
        primaryStore.close();
        primaryConn.close();
    }

    private Document getNewDocument(Connection connection) {
        String newDocUUID = UUID.randomUUID().toString();
        return connection
                .newDocument()
                .setId("fdoe-" + newDocUUID)
                .set("name", "fredDoe-" + newDocUUID)
                .set("type", "user")
                .set("yelping_since", "2014-03-23")
                .set("fans", 2)
                .set("support", "gold");
    }

    private int countDocuments(DocumentStore store) {
        int counter = 0;
        for (Document ignored : store.find()) {
            counter++;
        }
        return counter;
    }

}
