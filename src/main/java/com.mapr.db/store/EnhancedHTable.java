package com.mapr.db.store;

import com.mapr.db.policy.RetryPolicy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.ClusterConnection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RowMutations;
import org.apache.hadoop.hbase.client.RpcRetryingCallerFactory;
import org.apache.hadoop.hbase.client.TableConfiguration;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.ipc.RpcControllerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class EnhancedHTable extends HTable {

  private RetryPolicy policy;

  public EnhancedHTable(TableName tableName, ClusterConnection connection, TableConfiguration tableConfig,
                        RpcRetryingCallerFactory rpcCallerFactory, RpcControllerFactory rpcControllerFactory,
                        ExecutorService pool, RetryPolicy retryPolicy) throws IOException {
    super(tableName, connection, tableConfig, rpcCallerFactory, rpcControllerFactory, pool);
    this.policy = retryPolicy;
  }

  public EnhancedHTable(Configuration configuration, String table, RetryPolicy retryPolicy) throws IOException {
    super(configuration, table);
    this.policy = retryPolicy;
  }

  public Result append(final Append append, final RetryPolicy retryPolicy) throws IOException {
    super.append(append);
    return null;
  }

  public boolean checkAndDelete(final byte[] row,
                                final byte[] family, final byte[] qualifier, final byte[] value,
                                final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    super.checkAndDelete(row, family, qualifier, value, delete);
    return false;
  }

  public boolean checkAndDelete(final byte [] row, final byte [] family,
                                final byte [] qualifier, final CompareFilter.CompareOp compareOp, final byte [] value,
                                final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    super.checkAndDelete(row, family, qualifier, compareOp, value, delete);
    return false;
  }

  public boolean checkAndMutate(final byte[] row, final byte[] family, final byte[] qualifier,
                                final CompareFilter.CompareOp compareOp, final byte[] value,
                                final RowMutations rm, final RetryPolicy retryPolicy)
      throws IOException {
    super.checkAndMutate(row, family, qualifier, compareOp, value, rm);
    return false;
  }

  public boolean checkAndPut(final byte[] row,
                             final byte[] family, final byte[] qualifier, final byte[] value,
                             final Put put, final RetryPolicy retryPolicy)
      throws IOException {
    super.checkAndPut(row, family, qualifier, value, put);
    return false;
  }

  public void delete(final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    super.delete(delete);
  }

  public void delete(final List<Delete> deletes, final RetryPolicy retryPolicy)
      throws IOException {
    super.delete(deletes);
  }

  public boolean exists(final Get get, final RetryPolicy retryPolicy) throws IOException {
    super.exists(get);
    return false;
  }

  public boolean[] existsAll(final List<Get> gets, final RetryPolicy retryPolicy) throws IOException {
    existsAll(gets);
    return null;
  }

  public Boolean[] exists(final List<Get> gets, final RetryPolicy retryPolicy) throws IOException {
    exists(gets);
    return null;
  }

  public void mutateRow(final RowMutations rm, final RetryPolicy retryPolicy) throws IOException {
    mutateRow(rm);
  }

  public void put(final Put put, final RetryPolicy retryPolicy) throws IOException {
    put(put);
  }

}
