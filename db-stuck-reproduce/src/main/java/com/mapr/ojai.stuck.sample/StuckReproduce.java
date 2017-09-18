package com.mapr.ojai.stuck.sample;

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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SuppressWarnings("Duplicates")
public class StuckReproduce {

    private static final Logger LOG =
            LoggerFactory.getLogger(StuckReproduce.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException, TimeoutException {


        Connection primConn = DriverManager.getConnection("ojai:mapr:");
        DocumentStore primary = primConn.getStore("/mapr/mapr.cluster4/apps/user_profiles_");

        Connection secConn = DriverManager.getConnection("ojai:mapr:");
        DocumentStore secondary = secConn.getStore("/mapr/mapr.cluster3/apps/user_profiles_");

        ExecutorService prim = Executors.newFixedThreadPool(2);

        int counter = 0;
        for (int i = 0; i < 5; i++) {
            counter++;
            Document doc = generateDocument(counter, primConn);
            LOG.info("Inserting {} document {}. ", counter, doc.getId());
            prim.submit(()-> primary.insert(doc));
        }


        // On that moment need to run mapr-warden stop on the primary cluster
        sleep(90000);

        Document doc = generateDocument(counter, primConn);
        LOG.info("Inserting {} document {}. ", counter, doc.getId());

        try {
            prim.submit(() -> primary.insert(doc));
            prim.submit(() -> secondary.insert(doc)).get(500, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        prim.shutdownNow();
    }

    private static Document generateDocument(int id, Connection connection) {
        String newDocUUID = UUID.randomUUID().toString();

        String value = String.format("sample-%05d-%s", id, newDocUUID);

        return connection
                .newDocument()
                .setId(value)
                .set("name", value);
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException ignored) {
        }
    }
}