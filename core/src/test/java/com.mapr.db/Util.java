package com.mapr.db;

import com.google.common.io.Resources;
import org.apache.commons.lang3.time.StopWatch;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.concurrent.Callable;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class Util {

    private static final Logger LOG = LoggerFactory.getLogger(Util.class);

    /**
     * Interval for checking condition in the waitUntil() methods
     */
    private static final int DEFAULT_POLL_INTERVAL = 25000;

    /**
     * Run shell script that will restart mapr-warden service on a cluster.
     *
     * @param hostWithPrimaryCluster host for ssh connection to the cluster
     * @param passForRoot            password for ssh connection as a root user to the cluster
     */
    static void startRestartingWarden(String hostWithPrimaryCluster, String passForRoot) {
        new Thread(() -> restartWarden(hostWithPrimaryCluster, passForRoot)).start();
    }

    static void printClustersInfo(String primaryCluster, String failoverCluster) {
        LOG.info("Primary Cluster -> {}", primaryCluster);
        LOG.info("FailOver Cluster -> {}", failoverCluster);
    }

    /**
     * This method will remove all data from the primary and failover tables.
     * For correct work it need both of available clusters, if clusters will not available method will
     * thrown the Exception.
     *
     * @param primaryTable  Name of the primary table
     * @param failoverTable Name of the failover table
     */
    static void clearTable(String primaryTable, String failoverTable) {
        Connection connection = DriverManager.getConnection("ojai:mapr:");

        DocumentStore storeMaster = connection.getStore(primaryTable);
        DocumentStore storeFailOver = connection.getStore(failoverTable);

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
    }

    /**
     * Simple wrapper for standard sleep method that remove checked exception
     *
     * @param millis Time for sleep
     */
    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }

    /**
     * This method check condition during timeout.
     * If time out and condition not true, will be thrown the AssertionError
     *
     * @param timeout   time for checking
     * @param condition Condition that want to check
     */
    static void waitUntil(int timeout, Callable<Boolean> condition) {
        waitUntil(timeout, DEFAULT_POLL_INTERVAL, condition);
    }

    static void waitUntil(int timeout, int pollInterval, Callable<Boolean> condition) {
        waitUntil(timeout, pollInterval, "Waiting for condition timed out", condition);
    }

    static void waitUntil(int timeout, String failureMessage, Callable<Boolean> condition) {
        waitUntil(timeout, DEFAULT_POLL_INTERVAL, failureMessage, condition);
    }

    static void waitUntil(int timeout, int pollInterval, String failureMessage, Callable<Boolean> condition) {
        StopWatch stopwatch = StopWatch.createStarted();
        while (stopwatch.getTime(MILLISECONDS) < timeout) {
            if (isConditionSuccess(condition)) {
                LOG.info("SWAPPING SUCCESS");
                return;
            }
            sleep(pollInterval);
        }
        throw new AssertionError(failureMessage);
    }

    static void waitUntilPass(int timeout, int pollInterval, Runnable task) {
        AssertionError lastAssertionError;
        StopWatch stopwatch = StopWatch.createStarted();
        do {
            try {
                task.run();
                return;
            } catch (AssertionError e) {
                lastAssertionError = e;
            }
            sleep(pollInterval);
        } while (stopwatch.getTime(MILLISECONDS) < timeout);
        throw lastAssertionError;
    }

    private static void restartWarden(String hostWithPrimaryCluster, String rootPass) {
        URL restartScript = Resources.getResource("restartWarden.sh");
        ProcessBuilder pb = new ProcessBuilder(restartScript.getPath(), hostWithPrimaryCluster, rootPass);
        try {
            Process p = pb.start();
            BufferedReader reader = new BufferedReader(new InputStreamReader(p.getErrorStream()));
            String line;
            while ((line = reader.readLine()) != null) {
                LOG.info(line);
            }
        } catch (IOException io) {
            LOG.info(io.getMessage());
        }
    }

    private static Boolean isConditionSuccess(Callable<Boolean> condition) {
        try {
            return condition.call();
        } catch (Exception e) {
            throw new RuntimeException(e.getCause());
        }
    }

    static void waitUntilPass(int timeout, Runnable task) {
        waitUntilPass(timeout, DEFAULT_POLL_INTERVAL, task);
    }

}
