package com.mapr.db;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mapr.db.Util.clearTable;
import static com.mapr.db.Util.printClustersInfo;
import static com.mapr.db.Util.sleep;
import static com.mapr.db.Util.restartingNetManager;
import static com.mapr.db.Util.waitUntil;

@Category(IntegrationTest.class)
public class InsertDocumentsTest implements IntegrationTest {
    private static final Logger LOG = LoggerFactory.getLogger(InsertDocumentsTest.class);

    /**
     * The name of the primary table
     */
    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    /**
     * The name of the failover table
     */
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";
    /**
     * The host name of the cluster on which we want to reload network manager
     */
    private static final String HOST_WITH_PRIMARY_CLUSTER = "node1";
    /**
     * The password for the root user in the cluster on which we want to reload network manager
     */
    private static final String PASSWORD_FOR_ROOT = "mapr";
    /**
     * The quantity of the inserts documents to the db
     */
    private static final int NUMBER_OF_INSERTS = 5_000_000;
    /**
     * Used for setting time for checking swapping of the tables
     */
    private static final int TIMEOUT_FOR_CHECKING_CONDITION = 120_000;

    private AtomicInteger totalCounter;     // Total quantity of the inserts

    private EnhancedJSONTable jsonTable;    // Client for working with db

    private LocalDateTime dateTimeStart;    // Used for determining the moment for restarting network manager
    private LocalDateTime retryTime;        // time for the restarting network manager

    private DocumentStore failover;         // Used for getting documents from failover table

    private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

    @Before
    public void setup() {
        dateTimeStart = LocalDateTime.now();
        retryTime = dateTimeStart.plusMinutes(1);
        // Create instance of client for failover table for retrieving data from table
        failover = connection.getStore(FAILOVER_TABLE);
        // Create atomic integer for tracking quantity of the inserts
        totalCounter = new AtomicInteger();
        // Create an "Enhanced" data store that support fail over to other cluster
        jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);
        printClustersInfo(PRIMARY_TABLE, FAILOVER_TABLE);
    }

    @Test
    public void testSwappingClusterIfPrimaryClusterDownAndSwapBackWhenPrimaryClusterWillReload() {
        // create and run inserting data to the store
        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> insertRandomData(jsonTable, NUMBER_OF_INSERTS, 1500));
        // Reload Network Manager on the primary cluster
        restartingNetManager(HOST_WITH_PRIMARY_CLUSTER, PASSWORD_FOR_ROOT);
        // Wait for the switching to the failover cluster
        waitUntil(TIMEOUT_FOR_CHECKING_CONDITION, jsonTable::isTableSwitched);
        // Wait for the switching back to the primary cluster
        waitUntil(TIMEOUT_FOR_CHECKING_CONDITION, () -> !jsonTable.isTableSwitched());
        executor.shutdownNow();
    }

    @Test
    public void testFailoverWithALotOfData() throws InterruptedException {
        totalCounter.set(0);
        startInsertingDataFromDiffThreads(jsonTable, 5, 100);

        ExecutorService executor = Executors.newSingleThreadExecutor();
        executor.submit(() -> startRandomRestartingNetMan(30 * 1000));

        while (totalCounter.get() != NUMBER_OF_INSERTS) {
        }

        LOG.info("Number of inserted data in failover table: {}", totalCounter.get());
        DocumentStream documents = failover.find();
        int totalDocInDb = countRecords(documents);
        LOG.info("Number of documents in the db: {}", totalDocInDb);

        Assert.assertTrue("Some documents are not inserted", totalDocInDb >= totalCounter.get());
    }

    /**
     * @param jsonTable       the store to which the documents will be added
     * @param numberOfThreads amount of the threads for inserting data
     * @param pause           the time between the inserting
     */
    private void startInsertingDataFromDiffThreads(DocumentStore jsonTable, int numberOfThreads, long pause) {
        ExecutorService executor = Executors.newFixedThreadPool(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            executor.submit(() -> insertRandomData(jsonTable, NUMBER_OF_INSERTS, pause));
        }
    }

    /**
     * This method runs the script with command for restarting network manager on the cluster.
     * It restarts net man in the random time in range from 1 to 3 minutes.
     *
     * @param pauseBtwChecks time between checks of the condition
     */
    private void startRandomRestartingNetMan(long pauseBtwChecks) {
        // In the infinite loop we in a random period of time try to restart network manager
        while (true) {
            // check if the time for restarting did not come, and sleep some time
            if (LocalDateTime.now().isBefore(retryTime)) {
                sleep(pauseBtwChecks);
                continue;
            }
            restartingNetManager(HOST_WITH_PRIMARY_CLUSTER, PASSWORD_FOR_ROOT);
            // determine time for the next restart
            int randomNum = ThreadLocalRandom.current().nextInt(1, 3 + 1);
            dateTimeStart = LocalDateTime.now();
            retryTime = dateTimeStart.plusMinutes(randomNum);
        }
    }

    /**
     * @param store  the store to which the documents will be added
     * @param amount quantity of documents
     * @param pause  the time between the inserting
     */
    private void insertRandomData(DocumentStore store, int amount, long pause) {
        int counter = totalCounter.incrementAndGet();

        while (counter < amount) {
            counter++;
            Document doc = generateDocument(counter);
            LOG.info("Inserting {} document {}. ", counter, doc.getId());

            store.insert(doc);

            sleep(pause);
            totalCounter.incrementAndGet();
        }
    }

    /**
     * @param stream stream with documents from db
     * @return quantity of the documents in the stream
     */
    private int countRecords(DocumentStream stream) {
        int counter = 0;
        for (final Document ignored : stream) {
            counter++;
        }
        return counter;
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
        clearTable(PRIMARY_TABLE, FAILOVER_TABLE);
        jsonTable.close();
    }
}


