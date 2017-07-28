package com.mapr.db;

import com.mapr.db.policy.RetryPolicy;
import com.mapr.db.store.EnhancedHTable;
import com.mapr.fs.RetryException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.mapr.AbstractHTable;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestEnhancedHTable {

    private HTable hTable;

    public TestEnhancedHTable() throws IOException {
        Configuration configuration = HBaseConfiguration.create();
        RetryPolicy policy = RetryPolicy.builder()
                .numberOfRetries(50)
                .timeout(50000)
                .alternateSuffix(".foo")
                .alternateTable("/some/different/table")
                .build();
        this.hTable = new EnhancedHTable(configuration, "/apps/table_1", policy);
    }

    @Test
    public void testAppend() throws IOException {
        AbstractHTable abstractHTableMock;
        abstractHTableMock = mock(AbstractHTable.class);

        Append append = mock(Append.class);

        when(abstractHTableMock.append(append))
                .thenThrow(RetryException.class);

        hTable.append(append);
    }
}
