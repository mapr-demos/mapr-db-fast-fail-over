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

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String SECONDARY_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    private static final int TIMEOUT = 500;

    public static void main(String[] args) throws IOException, ExecutionException, InterruptedException {

        LOG.info("\n=== START ===");

        EnhancedJSONTable jsonTable =
                new EnhancedJSONTable(PRIMARY_TABLE, SECONDARY_TABLE, TIMEOUT);

        LOG.info("Primary Table: " + PRIMARY_TABLE);
        LOG.info("Secondary Table: " + SECONDARY_TABLE);
        LOG.info("Timeout: " + TIMEOUT);

        new Sample().testInsert(jsonTable);
        new Sample().testFind(jsonTable);

        jsonTable.close();

        LOG.info("\n=== FINISH ===");
    }

    public void testFind(EnhancedJSONTable jsonTable) {
        DocumentStream stream = jsonTable.find();
        LOG.info("Quantity of records: " + countDocuments(stream));
    }

    public void testInsert(EnhancedJSONTable jsonTable) throws InterruptedException {
        Connection primaryConn =
                DriverManager.getConnection(DRIVER_NAME);

        LOG.info("Make requests to the cluster, when both of clusters work");
        makeRequestToDb(jsonTable, primaryConn, 20);


            Thread.sleep(10 * 1000);
        LOG.info("SWITCH ON PRIMARY CLUSTER");
        makeRequestToDb(jsonTable, primaryConn, 20);

    }

    private void makeRequestToDb(EnhancedJSONTable jsonTable, Connection connection) {
        while (true) {
            Document doc = getNewDocument(connection);
            LOG.info("Insert - " + doc.asJsonString());
            jsonTable.insert(doc);
        }
    }

    private void makeRequestToDb(EnhancedJSONTable jsonTable, Connection connection,
                                 int quantityOfRequest) {
        for (int i = 0; i < quantityOfRequest; i++) {
            Document doc = getNewDocument(connection);
            LOG.info("Insert - " + doc.asJsonString());
            jsonTable.insert(doc);
        }
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