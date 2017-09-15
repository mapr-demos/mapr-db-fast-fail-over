package com.mapr.db.failover.samples;

import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DocumentStore;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;


public class StuckReproduce {

    private static final Logger LOG =
            LoggerFactory.getLogger(StuckReproduce.class);

    private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

    @SuppressWarnings("Duplicates")
    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {

        DocumentStore primary = connection.getStore("/mapr/mapr.cluster1/apps/user_profiles_new");
        DocumentStore secondary = connection.getStore("/mapr/mapr.cluster3/apps/user_profiles_new");

        ExecutorService prim = Executors.newFixedThreadPool(2);

        int counter = 0;

        while (true) {

            counter++;
            Document doc = generateDocument(counter);
            LOG.info("Inserting {} document {}. ", counter, doc.getId());
            Future<?> primaryFuture = prim.submit(() -> primary.insert(doc));
            try {
                primaryFuture. get(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                LOG.info("Insert to second table");
                prim.submit(() -> secondary.insert(doc)).get(500, TimeUnit.MILLISECONDS);
            }

        }
    }

    private static Document generateDocument(int id) {
        String newDocUUID = UUID.randomUUID().toString();

        String value = String.format("sample-%05d-%s", id, newDocUUID);

        return connection
                .newDocument()
                .setId(value)
                .set("name", value);
    }
}