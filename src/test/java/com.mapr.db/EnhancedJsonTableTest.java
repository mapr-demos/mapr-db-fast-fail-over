package com.mapr.db;

import org.junit.Ignore;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

public class EnhancedJsonTableTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(EnhancedJsonTableTest.class);

//    private static final int NUMBER_OF_REQUESTS = 100;

    private static final String DRIVER_NAME = "ojai:mapr:";

    private static final String PRIMARY_CLUSTER = "";
    private static final String SECONDARY_CLUSTER = "";

    private static final String PRIMARY_TABLE = "/mapr/cluster1/apps/user_profile";
    private static final String SECONDARY_TABLE = "/mapr/cluster2/apps/user_profile";

    private static final int TIMEOUT = 500;

    //    TODO: Find method to slow connection with primary cluster or shutdown it from test
    @Test
    @Ignore("Needs configuration data for the two reachable clusters")
    public void testInsert() throws InterruptedException {
        Connection primaryConn =
                DriverManager.getConnection(DRIVER_NAME + PRIMARY_CLUSTER);
        Connection secondaryConn =
                DriverManager.getConnection(DRIVER_NAME + SECONDARY_CLUSTER);

        DocumentStore primaryStore = primaryConn.getStore(PRIMARY_TABLE);
        DocumentStore secondaryStore = secondaryConn.getStore(SECONDARY_TABLE);

        EnhancedJSONTable jsonTable =
                new EnhancedJSONTable(PRIMARY_CLUSTER, PRIMARY_TABLE,
                        SECONDARY_CLUSTER, SECONDARY_TABLE, TIMEOUT);

        LOG.info("Make requests to the cluster, when both of clusters work");
        makeRequestToDb(jsonTable, primaryConn, 10);
        LOG.info("SHUTDOWN CLUSTER FOR A SOME TIME");
        Thread.sleep(10 * 1000);

        LOG.info("Make requests to the cluster, when primary cluster shutdown");
        makeRequestToDb(jsonTable, primaryConn, 30);

        Thread.sleep(10 * 1000);
        LOG.info("SWITCH ON PRIMARY CLUSTER");
        makeRequestToDb(jsonTable, primaryConn, 10);


//        Assert.assertEquals(0, countDocuments(primaryStore));
//        Assert.assertEquals(NUMBER_OF_REQUESTS, countDocuments(secondaryStore));

        closeStoreAndConn(primaryConn, primaryStore);
        closeStoreAndConn(secondaryConn, secondaryStore);
    }

    private void makeRequestToDb(EnhancedJSONTable jsonTable, Connection connection,
                                 int quantityOfRequest) {
        for (int i = 0; i < quantityOfRequest; i++) {
            Document doc = getNewDocument(connection);
            LOG.info("Insert - " + doc.asJsonString());
            jsonTable.insert(doc);
        }
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
