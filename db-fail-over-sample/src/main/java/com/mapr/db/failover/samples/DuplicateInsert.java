package com.mapr.db.failover.samples;

import com.mapr.db.EnhancedJSONTable;
import org.ojai.Document;
import org.ojai.store.Connection;
import org.ojai.store.DriverManager;
import org.ojai.store.exceptions.DocumentExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DuplicateInsert {

    private static final Logger LOG =
            LoggerFactory.getLogger(DuplicateInsert.class);

    private static final String PRIMARY_TABLE = "/mapr/mapr.cluster1/apps/user_profiles";
    private static final String FAILOVER_TABLE = "/mapr/mapr.cluster2/apps/user_profiles";

    // Create an OJAI connection to MapR cluster
    private static final Connection connection = DriverManager.getConnection("ojai:mapr:");

    public static void main(String[] args) throws IOException, InterruptedException {

        LOG.info(" ==== Start Application ===");

        // Create an "Enhanced" data store that support fail over to other cluster
        EnhancedJSONTable jsonTable = new EnhancedJSONTable(PRIMARY_TABLE, FAILOVER_TABLE);

        Document doc = connection.newDocument("{\"_id\" : \"sample-01\", \"name\" : \"sample-01\"}");

        LOG.info("Deleting sample ");
        jsonTable.delete("sample-01");
        // jsonTable.flush(); // TODO : Solved with issue #23

        //insert 1
        LOG.info("Inserting document a first time ");
        jsonTable.insert(doc);

        try {
            // insert 2
            LOG.info("Inserting document a second time, should fail ");
            jsonTable.insert(doc);
        } catch (DocumentExistsException e) {
            LOG.error("Cannot insert : document exists - {}", e.getMessage());
        }

        jsonTable.close();

        LOG.info(" ==== Stop Application ===");
    }
}
