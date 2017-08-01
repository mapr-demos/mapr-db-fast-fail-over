package com.mapr.db;

import org.ojai.Document;
import org.ojai.store.DocumentStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.mapr.db.Util.createAndExecuteTaskForSwitchingTableBack;
import static com.mapr.db.Util.getJsonTable;

public class EnhancedJSONTable {

    private static final Logger log =
            LoggerFactory.getLogger(EnhancedBinaryTable.class);

    private DocumentStore primary;
    private DocumentStore secondary;

    private long timeOut;

    private AtomicBoolean switched =
            new AtomicBoolean(false);

    public EnhancedJSONTable(String primaryClusterURL, String primaryTable,
                             String secondaryClusterURL, String secondaryTable, long timeOut) {
        this.primary =
                getJsonTable(primaryClusterURL, primaryTable);
        this.secondary =
                getJsonTable(secondaryClusterURL, secondaryTable);
        this.timeOut = timeOut;
    }

    public void insert(Document document) throws IOException, InterruptedException, ExecutionException {
        if (!switched.get()) {
            CompletableFuture<Void> primaryFuture =
                    CompletableFuture.runAsync(() -> primary.insert(document));
            primaryFuture.exceptionally(throwable -> {
                log.error("Problem while execution", throwable);
                throw new RuntimeException(throwable);
            });

            try {
                primaryFuture.get(timeOut, TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {

                log.error("Processing request to primary table too long");
                log.info("Switched table to secondary for a 1000 ms");

                switched.set(true);
                createAndExecuteTaskForSwitchingTableBack(switched);

                log.info("Try to perform operation with secondary table");
                CompletableFuture<Void> secondaryFuture =
                        CompletableFuture.runAsync(() -> secondary.insert(document));

                secondaryFuture.exceptionally(throwable -> {
                    log.error("Problem while execution", throwable);
                    throw new RuntimeException(throwable);
                });
                secondaryFuture.acceptEitherAsync(primaryFuture, s -> { });
            }
        } else {
            log.info("Perform operation with second table, because primary too slow");
            secondary.insert(document);
        }
    }
}
