package com.mapr.db.store;

import com.mapr.db.exception.RetryPolicyException;
import com.mapr.db.policy.RetryPolicy;
import com.mapr.fs.RetryException;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class EnhancedHTable extends HTable {

  private static final int END_OF_RETRIES_FLAG = -1;
  private static final int LAST_RETRY_FLAG = 0;

  private RetryPolicy policy;
  private int tempNumOfRetries = END_OF_RETRIES_FLAG;

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
    try {
      setupQuantityOfRetries(retryPolicy);
      Result result = super.append(append);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
      return result;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      return append(append, retryPolicy);
    }
  }

  @Override
  public Result append(final Append append) throws IOException {
    return append(append, policy);
  }

  public boolean checkAndDelete(final byte[] row,
                                final byte[] family, final byte[] qualifier, final byte[] value,
                                final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      boolean result = super.checkAndDelete(row, family, qualifier, value, delete);
      tempNumOfRetries = -1;
      return result;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      return checkAndDelete(row, family, qualifier, value, delete, retryPolicy);
    }
  }

  @Override
  public boolean checkAndDelete(final byte[] row,
                                final byte[] family, final byte[] qualifier, final byte[] value,
                                final Delete delete)
      throws IOException {
    return checkAndDelete(row, family, qualifier, value, delete, policy);
  }

  public boolean checkAndDelete(final byte[] row, final byte[] family,
                                final byte[] qualifier, final CompareFilter.CompareOp compareOp, final byte[] value,
                                final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      boolean result = super.checkAndDelete(row, family, qualifier, compareOp, value, delete);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
      return result;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      return checkAndDelete(row, family, qualifier, compareOp, value, delete, retryPolicy);
    }
  }

  @Override
  public boolean checkAndDelete(final byte[] row, final byte[] family,
                                final byte[] qualifier, final CompareFilter.CompareOp compareOp, final byte[] value,
                                final Delete delete)
      throws IOException {
    return checkAndDelete(row, family, qualifier, compareOp, value, delete, policy);
  }

  public boolean checkAndMutate(final byte[] row, final byte[] family, final byte[] qualifier,
                                final CompareFilter.CompareOp compareOp, final byte[] value,
                                final RowMutations rm, final RetryPolicy retryPolicy)
      throws IOException {
    CompletableFuture<Boolean> completeFuture =
        CompletableFuture.supplyAsync(() -> {

          setupQuantityOfRetries(retryPolicy);
          while (tempNumOfRetries != 0) {
            try {
              return super.checkAndMutate(row, family, qualifier, compareOp, value, rm);
            } catch (IOException ignored) {
            }
            tempNumOfRetries--;
          }
          throw new RuntimeException();
        });
    try {
      return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      throw new RetryException(e);
    }
  }

  @Override
  public boolean checkAndMutate(final byte[] row, final byte[] family, final byte[] qualifier,
                                final CompareFilter.CompareOp compareOp, final byte[] value,
                                final RowMutations rm)
      throws IOException {
    return checkAndMutate(row, family, qualifier, compareOp, value, rm, policy);
  }

  public boolean checkAndPut(final byte[] row,
                             final byte[] family, final byte[] qualifier, final byte[] value,
                             final Put put, final RetryPolicy retryPolicy)
      throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      boolean result = super.checkAndPut(row, family, qualifier, value, put);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
      return result;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      return checkAndPut(row, family, qualifier, value, put, retryPolicy);
    }
  }

  @Override
  public boolean checkAndPut(final byte[] row,
                             final byte[] family, final byte[] qualifier, final byte[] value,
                             final Put put)
      throws IOException {
    return checkAndPut(row, family, qualifier, value, put, policy);
  }

  public void delete(final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      super.delete(delete);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      delete(delete, retryPolicy);
    }
  }

  @Override
  public void delete(final Delete delete)
      throws IOException {
    delete(delete, policy);
  }

  public void delete(final List<Delete> deletes, final RetryPolicy retryPolicy)
      throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      super.delete(deletes);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      delete(deletes, retryPolicy);
    }
  }

  @Override
  public void delete(final List<Delete> deletes)
      throws IOException {
    delete(deletes, policy);
  }

  public boolean exists(final Get get, final RetryPolicy retryPolicy) throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      boolean result = super.exists(get);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
      return result;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      return exists(get, retryPolicy);
    }
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    return exists(get, policy);
  }

  public boolean[] existsAll(final List<Get> gets, final RetryPolicy retryPolicy) throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      boolean[] result = super.existsAll(gets);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
      return result;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      return existsAll(gets, retryPolicy);
    }
  }

  @Override
  public boolean[] existsAll(final List<Get> gets) throws IOException {
    return existsAll(gets, policy);
  }

  public Boolean[] exists(final List<Get> gets, final RetryPolicy retryPolicy) throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      Boolean[] result = super.exists(gets);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
      return result;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      return exists(gets, retryPolicy);
    }
  }

  @Override
  public Boolean[] exists(final List<Get> gets) throws IOException {
    return exists(gets, policy);
  }

  public void mutateRow(final RowMutations rm, final RetryPolicy retryPolicy) throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      super.mutateRow(rm);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      mutateRow(rm, retryPolicy);
    }
  }

  @Override
  public void mutateRow(final RowMutations rm) throws IOException {
    mutateRow(rm, policy);
  }

  public void put(final Put put, final RetryPolicy retryPolicy) throws IOException {
    try {
      setupQuantityOfRetries(retryPolicy);
      super.put(put);
      tempNumOfRetries = END_OF_RETRIES_FLAG;
    } catch (RetryException rpe) {
      checkConditionsForRetry();
      put(put, retryPolicy);
    }
  }

  @Override
  public void put(final Put put) throws IOException {
    put(put, policy);
  }

  private void setupQuantityOfRetries(RetryPolicy policy) {
    if (tempNumOfRetries == END_OF_RETRIES_FLAG) {
      tempNumOfRetries = policy.getNumOfRetries();
    }
  }

  private void checkConditionsForRetry() {
    if (tempNumOfRetries == LAST_RETRY_FLAG) {
      throw new RetryPolicyException();
    }
    tempNumOfRetries--;
  }
}
