package com.mapr.db;

import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class Sample {

    private static final Logger LOG =
            LoggerFactory.getLogger(Sample.class);

    private static final String DRIVER_NAME = "ojai:mapr:";

    private static final String PRIMARY_CLUSTER = "";
    private static final String SECONDARY_CLUSTER = "";

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String SECONDARY_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    private static final int TIMEOUT = 5;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        System.out.println("\n=== START ===");

        new Sample().testInsert();
        new Sample().testFind();

        System.out.println("\n=== FINISH ===");
    }

    public void testFind() {
        EnhancedJSONTable jsonTable =
                new EnhancedJSONTable(PRIMARY_CLUSTER, PRIMARY_TABLE,
                        SECONDARY_CLUSTER, SECONDARY_TABLE, TIMEOUT);
        DocumentStream stream = jsonTable.find();
        LOG.info("Quantity of records: " + countDocuments(stream));
    }

    public void testInsert() throws InterruptedException {
        Connection primaryConn =
                DriverManager.getConnection(DRIVER_NAME + PRIMARY_CLUSTER);

        EnhancedJSONTable jsonTable =
                new EnhancedJSONTable(PRIMARY_CLUSTER, PRIMARY_TABLE,
                        SECONDARY_CLUSTER, SECONDARY_TABLE, TIMEOUT);

        LOG.info("Make requests to the cluster, when both of clusters work");
        makeRequestToDb(jsonTable, primaryConn, 10);


            Thread.sleep(10 * 1000);
        LOG.info("SWITCH ON PRIMARY CLUSTER");
        makeRequestToDb(jsonTable, primaryConn, 10);


//        Assert.assertEquals(0, countDocuments(primaryStore));
//        Assert.assertEquals(NUMBER_OF_REQUESTS, countDocuments(secondaryStore));

//        closeStoreAndConn(primaryConn, primaryStore);
//        closeStoreAndConn(secondaryConn, secondaryStore);
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

    private int countDocuments(DocumentStream stream) {
        int counter = 0;
        for (Document ignored : stream) {
            counter++;
        }
        return counter;
    }
}