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

    private RetryPolicy policy;

    private AtomicBoolean switched =
            new AtomicBoolean(false);

    public EnhancedJSONTable(RetryPolicy policy) {
        this.primary = getJsonTable(policy.getPrimaryTable());
        this.secondary = getJsonTable(policy.getAlternateTable());
        this.policy = policy;
    }

    public void insert(Document document) throws IOException, InterruptedException, ExecutionException {
        if (!switched.get()) {
            CompletableFuture<Void> completableFuture =
                    CompletableFuture.supplyAsync(() -> {
                        primary.insert(document);
                        return null;
                    });
            completableFuture.exceptionally(throwable -> {
                log.error("Problem", throwable);
                return null;
            });
            try {
                completableFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
            } catch (TimeoutException e) {
                switched.set(true);
                createAndExecuteTaskForSwitchingTableBack(switched);
                secondary.insert(document);
            }
        } else {
            secondary.insert(document);
        }


    }
}
