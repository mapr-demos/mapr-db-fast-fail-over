package com.mapr.db;

import com.mapr.db.exceptions.DBException;
import com.mapr.db.impl.BaseJsonTable;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.ojai.Document;
import org.ojai.DocumentStream;
import org.ojai.FieldPath;
import org.ojai.Value;
import org.ojai.annotation.API;
import org.ojai.store.*;
import org.ojai.store.exceptions.MultiOpException;
import org.ojai.store.exceptions.StoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.*;

public class EnhancedJsonTableTest {

    private static final Logger LOG =
            LoggerFactory.getLogger(EnhancedJsonTableTest.class);

    @Test
    public void testPrimarySlowNoTimeout() {
        runSamples(5, 0, 100, 0, 0, false, false, 100);
    }

    @Test
    public void testPrimaryTimeout() {
        runSamples(40, 1, 0, 100, 100, false, false, 100);
    }

    @Test
    public void testBothSlow() throws Exception {
        try {
            runSamples(1100, 1050, 0, 0, 5, false, false, 5);
            fail("Expected failure due to timeout");
        } catch ( EnhancedJSONTable.FailoverException e) {
            assertTrue(e.getMessage().matches(".*primary and secondary.*"));
        }
    }

    @Test
    public void testAfailsBsucceeds() throws Exception {
        runSamples(20, 30, 0, 10, 10, true, false, 10);
    }

    private void runSamples(int aDelay, int bDelay, int primary, int secondary, int failed, boolean failA, boolean failB, int iterations) {
        int[] counts = runSamples((DocumentStore t) -> {
                int x = ((TestStore) t).tag;
                if (x == 0) {
                    Thread.sleep(aDelay);
                    if (failA) {
                        throw new StoreException("A has failed");
                    }
                } else {
                    Thread.sleep(bDelay);
                    if (failB) {
                        throw new StoreException("B has failed");
                    }
                }
                return x;
            }, iterations);
        assertEquals("Primary", primary, counts[0]);
        assertEquals("Secondary", secondary, counts[1]);
        assertEquals("Failed", failed, counts[2]);
    }

    private int[] runSamples(EnhancedJSONTable.TableFunction<Integer> task, int iterations) {
        ExecutorService prim = Executors.newSingleThreadExecutor();
        ExecutorService sec = Executors.newSingleThreadExecutor();
        TestStore a = new TestStore(0);
        TestStore b = new TestStore(1);

        int[] counts = new int[3];
        for (int i = 0; i < iterations; i++) {
            int r = EnhancedJSONTable.doWithFallback(
                    prim, sec, 20, 1000,
                    task,
                    a, b,
                    () -> counts[2]++, new AtomicBoolean(false));
            counts[r]++;
        }
        return counts;
    }

    private static class TestStore extends BaseJsonTable {
        int tag;

        TestStore(int tag) {
            super(new Configuration());
            this.tag = tag;
        }

        @Override
        protected void _doClose() {

        }

        @Override
        protected DocumentStream _doScan(QueryCondition queryCondition, String... strings) {
            return null;
        }

        @SuppressWarnings("deprecation")
        @Override
        protected TabletInfo[] _getTabletInfos(QueryCondition queryCondition, boolean b, boolean b1) throws IOException {
            return new TabletInfo[0];
        }

        @Override
        public TableType getTableType() {
            return null;
        }

        @Override
        public boolean isIndex() {
            return false;
        }

        @Override
        public Logger getLogger() {
            return null;
        }

        @Override
        public void findById(OpListener opListener, String s) {

        }

        @Override
        public void findById(OpListener opListener, ByteBuffer byteBuffer) {

        }

        @Override
        public void findById(OpListener opListener, String s, String... strings) {

        }

        @Override
        public void findById(OpListener opListener, String s, FieldPath... fieldPaths) {

        }

        @Override
        public void findById(OpListener opListener, ByteBuffer byteBuffer, String... strings) {

        }

        @Override
        public void findById(OpListener opListener, ByteBuffer byteBuffer, FieldPath... fieldPaths) {

        }

        @Override
        public void findById(OpListener opListener, String s, QueryCondition queryCondition) {

        }

        @Override
        public void findById(OpListener opListener, ByteBuffer byteBuffer, QueryCondition queryCondition) {

        }

        @Override
        public void findById(OpListener opListener, String s, QueryCondition queryCondition, String... strings) {

        }

        @Override
        public void findById(OpListener opListener, String s, QueryCondition queryCondition, FieldPath... fieldPaths) {

        }

        @Override
        public void findById(OpListener opListener, ByteBuffer byteBuffer, QueryCondition queryCondition, String... strings) {

        }

        @Override
        public void findById(OpListener opListener, ByteBuffer byteBuffer, QueryCondition queryCondition, FieldPath... fieldPaths) {

        }

        @Override
        public void insertOrReplace(Document document) throws DBException {

        }

        @Override
        public void insertOrReplace(String s, Document document) throws DBException {

        }

        @Override
        public void insertOrReplace(ByteBuffer byteBuffer, Document document) throws DBException {

        }

        @Override
        public void insertOrReplace(Document document, FieldPath fieldPath) throws DBException {

        }

        @Override
        public void insertOrReplace(Document document, String s) throws DBException {

        }

        @Override
        public void insertOrReplace(DocumentStream documentStream) throws MultiOpException {

        }

        @Override
        public void insertOrReplace(DocumentStream documentStream, FieldPath fieldPath) throws MultiOpException {

        }

        @Override
        public void insertOrReplace(DocumentStream documentStream, String s) throws MultiOpException {

        }

        @Override
        public void update(String s, DocumentMutation documentMutation) throws DBException {

        }

        @Override
        public void update(ByteBuffer byteBuffer, DocumentMutation documentMutation) throws DBException {

        }

        @Override
        public void delete(String s) throws DBException {

        }

        @Override
        public void delete(ByteBuffer byteBuffer) throws DBException {

        }

        @Override
        public void delete(Document document) throws DBException {

        }

        @Override
        public void delete(Document document, FieldPath fieldPath) throws DBException {

        }

        @Override
        public void delete(Document document, String s) throws DBException {

        }

        @Override
        public void delete(DocumentStream documentStream) throws MultiOpException {

        }

        @Override
        public void delete(DocumentStream documentStream, FieldPath fieldPath) throws MultiOpException {

        }

        @Override
        public void delete(DocumentStream documentStream, String s) throws MultiOpException {

        }

        @Override
        public void insert(String s, Document document) throws DBException {

        }

        @Override
        public void insert(ByteBuffer byteBuffer, Document document) throws DBException {

        }

        @Override
        public void insert(Document document) throws DBException {

        }

        @Override
        public void insert(Document document, FieldPath fieldPath) throws DBException {

        }

        @Override
        public void insert(Document document, String s) throws DBException {

        }

        @Override
        public void insert(DocumentStream documentStream) throws MultiOpException {

        }

        @Override
        public void insert(DocumentStream documentStream, FieldPath fieldPath) throws MultiOpException {

        }

        @Override
        public void insert(DocumentStream documentStream, String s) throws MultiOpException {

        }

        @Override
        public void replace(String s, Document document) throws DBException {

        }

        @Override
        public void replace(ByteBuffer byteBuffer, Document document) throws DBException {

        }

        @Override
        public void replace(Document document) throws DBException {

        }

        @Override
        public void replace(Document document, FieldPath fieldPath) throws DBException {

        }

        @Override
        public void replace(Document document, String s) throws DBException {

        }

        @Override
        public void replace(DocumentStream documentStream) throws MultiOpException {

        }

        @Override
        public void replace(DocumentStream documentStream, FieldPath fieldPath) throws MultiOpException {

        }

        @Override
        public void replace(DocumentStream documentStream, String s) throws MultiOpException {

        }

        @Override
        public void increment(String s, String s1, byte b) throws DBException {

        }

        @Override
        public void increment(ByteBuffer byteBuffer, String s, byte b) throws DBException {

        }

        @Override
        public void increment(String s, String s1, short i) throws DBException {

        }

        @Override
        public void increment(ByteBuffer byteBuffer, String s, short i) throws DBException {

        }

        @Override
        public void increment(String s, String s1, int i) throws DBException {

        }

        @Override
        public void increment(ByteBuffer byteBuffer, String s, int i) throws DBException {

        }

        @Override
        public void increment(String s, String s1, long l) throws DBException {

        }

        @Override
        public void increment(ByteBuffer byteBuffer, String s, long l) throws DBException {

        }

        @Override
        public void increment(String s, String s1, float v) throws DBException {

        }

        @Override
        public void increment(ByteBuffer byteBuffer, String s, float v) throws DBException {

        }

        @Override
        public void increment(String s, String s1, double v) throws DBException {

        }

        @Override
        public void increment(ByteBuffer byteBuffer, String s, double v) throws DBException {

        }

        @Override
        public void increment(String s, String s1, BigDecimal bigDecimal) throws DBException {

        }

        @Override
        public void increment(ByteBuffer byteBuffer, String s, BigDecimal bigDecimal) throws DBException {

        }

        @Override
        public boolean checkAndMutate(String s, QueryCondition queryCondition, DocumentMutation documentMutation) throws DBException {
            return false;
        }

        @Override
        public boolean checkAndMutate(ByteBuffer byteBuffer, QueryCondition queryCondition, DocumentMutation documentMutation) throws DBException {
            return false;
        }

        @Override
        public boolean checkAndDelete(String s, QueryCondition queryCondition) throws DBException {
            return false;
        }

        @Override
        public boolean checkAndDelete(ByteBuffer byteBuffer, QueryCondition queryCondition) throws DBException {
            return false;
        }

        @Override
        public boolean checkAndReplace(String s, QueryCondition queryCondition, Document document) throws DBException {
            return false;
        }

        @Override
        public boolean checkAndReplace(ByteBuffer byteBuffer, QueryCondition queryCondition, Document document) throws DBException {
            return false;
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public void insertOrReplace(@API.NonNullable Value _id, @API.NonNullable Document doc) throws StoreException {

        }

        @Override
        public void update(@API.NonNullable Value _id, @API.NonNullable DocumentMutation mutation) throws StoreException {

        }

        @Override
        public void delete(@API.NonNullable Value _id) throws StoreException {

        }

        @Override
        public void insert(@API.NonNullable Value _id, @API.NonNullable Document doc) throws StoreException {

        }

        @Override
        public void replace(@API.NonNullable Value _id, @API.NonNullable Document doc) throws StoreException {

        }

        @Override
        public void increment(@API.NonNullable Value _id, @API.NonNullable String field, byte inc) throws StoreException {

        }

        @Override
        public void increment(@API.NonNullable Value _id, @API.NonNullable String field, short inc) throws StoreException {

        }

        @Override
        public void increment(@API.NonNullable Value _id, @API.NonNullable String field, int inc) throws StoreException {

        }

        @Override
        public void increment(@API.NonNullable Value _id, @API.NonNullable String field, long inc) throws StoreException {

        }

        @Override
        public void increment(@API.NonNullable Value _id, @API.NonNullable String field, float inc) throws StoreException {

        }

        @Override
        public void increment(@API.NonNullable Value _id, @API.NonNullable String field, double inc) throws StoreException {

        }

        @Override
        public void increment(@API.NonNullable Value _id, @API.NonNullable String field, @API.NonNullable BigDecimal inc) throws StoreException {

        }

        @Override
        public boolean checkAndMutate(@API.NonNullable Value _id, @API.NonNullable QueryCondition condition, @API.NonNullable DocumentMutation mutation) throws StoreException {
            return false;
        }

        @Override
        public boolean checkAndDelete(@API.NonNullable Value _id, @API.NonNullable QueryCondition condition) throws StoreException {
            return false;
        }

        @Override
        public boolean checkAndReplace(@API.NonNullable Value _id, @API.NonNullable QueryCondition condition, @API.NonNullable Document doc) throws StoreException {
            return false;
        }
    }
}
