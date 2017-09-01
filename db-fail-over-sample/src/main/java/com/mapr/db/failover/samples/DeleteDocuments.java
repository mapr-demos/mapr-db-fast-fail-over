package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

@SuppressWarnings("Duplicates")
public class DeleteDocuments {

    private static final Logger LOG =
            LoggerFactory.getLogger(DeleteDocuments.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    // Create an OJAI connection to MapR cluster
    private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

    public static void main(String[] args) throws IOException {

        LOG.info("==== Start Application ====");

        // Create an "Enhanced" data store that support fail over to other cluster
        EnhancedJSONTable jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);

        LOG.info("==== Insert Documents ====");
        int counter = 0;
        while (counter < 1000) {
            counter++;
            Document doc = generateDocument(counter);
            LOG.info("Inserting {} document {}. ", counter, doc.getId());
            jsonTable.insertOrReplace(doc);
        }

        LOG.info("==== Delete Documents ====");

        // slowly delete document
        while (counter != 0) {
            counter--;
            String id = String.format("sample-%05d", counter);
            LOG.info("Deleting {}", id);
            jsonTable.delete(id);
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        jsonTable.close();
    }

    /**
     * Create document to be delete
     *
     * @param id integer part of the key and name
     * @return a new document with a ID from counter
     */
    private static Document generateDocument(int id) {
        String value = String.format("sample-%05d", id);

        return connection
                .newDocument()
                .setId(value)
                .set("name", value);
    }
}
