package com.mapr.db;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.mapr.db.Util.clearTable;
import static com.mapr.db.Util.printClustersInfo;
import static com.mapr.db.Util.sleep;
import static com.mapr.db.Util.startRestartingWarden;
import static com.mapr.db.Util.waitUntil;

@Category(IntegrationTest.class)
public class InsertDocumentsTest implements IntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(InsertDocumentsTest.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    private static final String HOST_WITH_PRIMARY_CLUSTER = "node1";
    private static final String PASSWORD_FOR_ROOT = "mapr";

    private static final int TIMEOUT_FOR_CHECKING_CONDITION = 120_0000000;

    private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

    private ExecutorService executor = Executors.newSingleThreadExecutor();

    private EnhancedJSONTable jsonTable;

    @Before
    public void setup() {
        printClustersInfo(PRIMARY_TABLE, FAILOVER_TABLE);
        // Create an "Enhanced" data store that support fail over to other cluster
        jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);

        executor.execute(() -> {
            boolean loop = true;
            int counter = 0;
            while (loop) {
                counter++;
                Document doc = generateDocument(counter);
                LOG.info("Inserting {} document {}. ", counter, doc.getId());
                jsonTable.insert(doc);

                // sleep for 1sec
                sleep(1500);
            }
        });
    }

    @Test
    public void testSwappingClusterIfPrimaryClusterDownAndSwapBackWhenPrimaryClusterWillReload() {
        // Reload Primary Cluster
        startRestartingWarden(HOST_WITH_PRIMARY_CLUSTER, PASSWORD_FOR_ROOT);
        // Wait for the switching to the failover cluster
        waitUntil(TIMEOUT_FOR_CHECKING_CONDITION, jsonTable::isTableSwitched);
        // Wait for the switching back to the primary cluster
        waitUntil(TIMEOUT_FOR_CHECKING_CONDITION, () -> !jsonTable.isTableSwitched());
    }

    /**
     * Create a "random" document
     *
     * @param id integer part of the key and name
     * @return a new document with a ID generated from a UUID
     */
    private static Document generateDocument(int id) {
        String newDocUUID = UUID.randomUUID().toString();

        String value = String.format("sample-%05d-%s", id, newDocUUID);

        return connection
                .newDocument()
                .setId(value)
                .set("name", value);
    }

    @After
    public void closeConnection() {
        executor.shutdownNow();
        clearTable(PRIMARY_TABLE, FAILOVER_TABLE);
        jsonTable.close();
    }
}


