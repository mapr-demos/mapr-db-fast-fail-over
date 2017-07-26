package com.mapr.db.store;

import com.mapr.db.exception.RetryPolicyException;
import com.mapr.db.policy.RetryPolicy;
import lombok.extern.slf4j.Slf4j;
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
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.mapr.db.Util.getTable;

@Slf4j
public class EnhancedHTable extends HTable {

  private final RetryPolicy policy;

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

  public EnhancedHTable(Configuration configuration, String table) throws IOException {
    super(configuration, table);
    this.policy = getPolicyFrom(configuration);
  }

  private RetryPolicy getPolicyFrom(Configuration configuration) throws IOException {
    return new ObjectMapper().readValue(configuration.get("mapr.db.retry.policy"), RetryPolicy.class);
  }

  @Override
  public Result append(final Append append) throws IOException {
    return append(append, policy);
  }

  public Result append(final Append append, final RetryPolicy retryPolicy) throws IOException {
    CompletableFuture<Result> completeFuture =
        CompletableFuture.supplyAsync(() -> {
          int numberOfRetries = retryPolicy.getNumOfRetries();
          while (numberOfRetries != 0) {
            try {
              return super.append(append);
            } catch (IOException ignored) {
              numberOfRetries--;
            }
          }
          throw new RuntimeException();
        });
    try {
      return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      throw new RetryPolicyException();
    }
  }

  @Override
  public boolean checkAndDelete(final byte[] row,
                                final byte[] family, final byte[] qualifier, final byte[] value,
                                final Delete delete)
      throws IOException {
    return checkAndDelete(row, family, qualifier, value, delete, policy);
  }

  public boolean checkAndDelete(final byte[] row,
                                final byte[] family, final byte[] qualifier, final byte[] value,
                                final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    Supplier<Boolean> checkAndDelete = () -> {
      try {
        return super.checkAndDelete(row, family, qualifier, value, delete);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };

    CompletableFuture<Boolean> completableFuture =
        processRequestWithRetries(checkAndDelete, retryPolicy);

    try {
      return completableFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      String tableName = getAlternativeTableName();

      Supplier<Boolean> checkAndDeleteInAlternativeTable = () -> {
        try {
          return getTable(tableName).checkAndDelete(row, family, qualifier, value, delete);
        } catch (IOException e1) {
          throw new RetryPolicyException(e1);
        }
      };
      return performWithAlternativeTable(checkAndDeleteInAlternativeTable);
    }
  }

  @Override
  public boolean checkAndDelete(final byte[] row, final byte[] family,
                                final byte[] qualifier, final CompareFilter.CompareOp compareOp, final byte[] value,
                                final Delete delete)
      throws IOException {
    return checkAndDelete(row, family, qualifier, compareOp, value, delete, policy);
  }

  public boolean checkAndDelete(final byte[] row, final byte[] family,
                                final byte[] qualifier, final CompareFilter.CompareOp compareOp, final byte[] value,
                                final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    Supplier<Boolean> checkAndDelete = () -> {
      try {
        return super.checkAndDelete(row, family, qualifier, compareOp, value, delete);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };
    CompletableFuture<Boolean> completeFuture = processRequestWithRetries(checkAndDelete, retryPolicy);
    try {
      return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      String tableName = getAlternativeTableName();
      Supplier<Boolean> checkAndDeleteInAltTable = () -> {
        try {
          return getTable(tableName).checkAndDelete(row, family, qualifier, compareOp, value, delete);
        } catch (IOException e1) {
          throw new RetryPolicyException(e1);
        }
      };
      return performWithAlternativeTable(checkAndDeleteInAltTable);
    }
  }

  @Override
  public boolean checkAndMutate(final byte[] row, final byte[] family, final byte[] qualifier,
                                final CompareFilter.CompareOp compareOp, final byte[] value,
                                final RowMutations rm)
      throws IOException {
    return checkAndMutate(row, family, qualifier, compareOp, value, rm, policy);
  }

  public boolean checkAndMutate(final byte[] row, final byte[] family, final byte[] qualifier,
                                final CompareFilter.CompareOp compareOp, final byte[] value,
                                final RowMutations rm, final RetryPolicy retryPolicy)
      throws IOException {
    Supplier<Boolean> checkAndMutate = () -> {
      try {
        return super.checkAndMutate(row, family, qualifier, compareOp, value, rm);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };
    CompletableFuture<Boolean> completeFuture =
        processRequestWithRetries(checkAndMutate, retryPolicy);
    try {
      return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      String tableName = getAlternativeTableName();
      Supplier<Boolean> checkAndMutateInAltTable = () -> {
        try {
          return getTable(tableName).checkAndMutate(row, family, qualifier, compareOp, value, rm);
        } catch (IOException e1) {
          throw new RetryPolicyException(e1);
        }
      };
      return performWithAlternativeTable(checkAndMutateInAltTable);
    }
  }

  @Override
  public boolean checkAndPut(final byte[] row,
                             final byte[] family, final byte[] qualifier, final byte[] value,
                             final Put put)
      throws IOException {
    return checkAndPut(row, family, qualifier, value, put, policy);
  }

  public boolean checkAndPut(final byte[] row,
                             final byte[] family, final byte[] qualifier, final byte[] value,
                             final Put put, final RetryPolicy retryPolicy)
      throws IOException {
    Supplier<Boolean> checkAndPut = () -> {
      try {
        return super.checkAndPut(row, family, qualifier, value, put);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };
    CompletableFuture<Boolean> completeFuture =
        processRequestWithRetries(checkAndPut, retryPolicy);
    try {
      return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      String tableName = getAlternativeTableName();
      Supplier<Boolean> checkAndPutInAltTable = () -> {
        try {
          return getTable(tableName).checkAndPut(row, family, qualifier, value, put);
        } catch (IOException e1) {
          throw new RetryPolicyException(e1);
        }
      };
      return performWithAlternativeTable(checkAndPutInAltTable);
    }
  }

  @Override
  public void delete(final Delete delete)
      throws IOException {
    delete(delete, policy);
  }

  public void delete(final Delete delete, final RetryPolicy retryPolicy)
      throws IOException {
    List<Delete> deletes = new LinkedList<>();
    deletes.add(delete);
    this.delete(deletes, retryPolicy);
  }

  @Override
  public void delete(final List<Delete> deletes)
      throws IOException {
    delete(deletes, policy);
  }

  public void delete(final List<Delete> deletes, final RetryPolicy retryPolicy)
      throws IOException {
    Consumer<Void> delete = s -> {
      try {
        super.delete(deletes);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };
    CompletableFuture<Void> completeFuture = processRequestWithRetries(delete, retryPolicy);
    try {
      completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      throw new RetryPolicyException();
    }
  }

  @Override
  public boolean exists(final Get get) throws IOException {
    return exists(get, policy);
  }

  public boolean exists(final Get get, final RetryPolicy retryPolicy) throws IOException {
    Supplier<Boolean> exists = () -> {
      try {
        return super.exists(get);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };

    CompletableFuture<Boolean> completeFuture =
        processRequestWithRetries(exists, retryPolicy);

    try {
      return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      String tableName = getAlternativeTableName();

      Supplier<Boolean> existsInAltTable = () -> {
        try {
          return getTable(tableName).exists(get);
        } catch (IOException e1) {
          throw new RetryPolicyException(e1);
        }
      };

      return performWithAlternativeTable(existsInAltTable);
    }
  }

  @Override
  public boolean[] existsAll(final List<Get> gets) throws IOException {
    return existsAll(gets, policy);
  }

  public boolean[] existsAll(final List<Get> gets, final RetryPolicy retryPolicy) throws IOException {
    CompletableFuture<boolean[]> completeFuture =
        CompletableFuture.supplyAsync(() -> {

          int numberOfRetries = retryPolicy.getNumOfRetries();

          while (numberOfRetries != 0) {
            try {
              return super.existsAll(gets);
            } catch (IOException ignored) {
              numberOfRetries--;
            }
          }

          throw new RuntimeException();
        });
    try {
      return completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      if (isAlternateTableExist(policy)) {
        return getTable(policy.getAlternateTable()).existsAll(gets);
      }
      throw new RetryPolicyException();
    }
  }

  @Override
  public void mutateRow(final RowMutations rm) throws IOException {
    mutateRow(rm, policy);
  }

  public void mutateRow(final RowMutations rm, final RetryPolicy retryPolicy) throws IOException {
    Consumer<Void> mutateRow = s -> {
      try {
        super.mutateRow(rm);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };

    CompletableFuture<Void> completeFuture =
        processRequestWithRetries(mutateRow, retryPolicy);

    try {
      completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      String tableName = getAlternativeTableName();

      Consumer<Void> mutateRowInAltTable = s -> {
        try {
          getTable(tableName).mutateRow(rm);
        } catch (IOException e1) {
          throw new RetryPolicyException(e1);
        }
      };
      performWithAlternativeTable(mutateRowInAltTable);
    }
  }

  @Override
  public void put(final Put put) throws IOException {
    put(put, policy);
  }

  public void put(final Put put, final RetryPolicy retryPolicy) throws IOException {
    Consumer<Void> putAction = s -> {
      try {
        super.put(put);
      } catch (IOException e) {
        throw new RetryPolicyException();
      }
    };

    CompletableFuture<Void> completeFuture =
        processRequestWithRetries(putAction, retryPolicy);

    try {
      completeFuture.get(policy.getTimeout(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      String tableName = getAlternativeTableName();

      Consumer<Void> putInAltTable = s -> {
        try {
          getTable(tableName).put(put);
        } catch (IOException e1) {
          throw new RetryPolicyException(e1);
        }
      };

      performWithAlternativeTable(putInAltTable);
    }
  }

  private boolean isAlternateTableExist(RetryPolicy policy) {
    return policy.getAlternateTable() != null;
  }

  private void performWithAlternativeTable(Consumer<Void> operation) {
    logInfoAboutAlternativeTable();
    operation.accept(null);
    log.info("Success!");
  }

  private Boolean performWithAlternativeTable(Supplier<Boolean> supplier) {
    logInfoAboutAlternativeTable();
    return supplier.get();
  }

  private void logInfoAboutAlternativeTable() {
    log.info("Failed operation with main table, path - {}", new String(getTableName()));
    log.info("Trying perform this operation with alternative table");
    log.info("path - {}", getAlternativeTableName());
  }

  private String getAlternativeTableName() {
    String tableName;
    if (isAlternateTableExist(policy)) {
      tableName = policy.getAlternateTable();
    } else {
      tableName = new String(getTableName()) + "_alternate";
    }
    return tableName;
  }

  private CompletableFuture<Void> processRequestWithRetries(Consumer<Void> consumer, RetryPolicy retryPolicy) {
    return CompletableFuture.supplyAsync(() -> {
      int numberOfRetries = retryPolicy.getNumOfRetries();
      while (numberOfRetries != 0) {
        try {
          consumer.accept(null);
          return null;
        } catch (RetryPolicyException rpe) {
          numberOfRetries--;
        }
      }
      throw new RetryPolicyException();
    });
  }

  private CompletableFuture<Boolean> processRequestWithRetries(Supplier<Boolean> supplier, RetryPolicy retryPolicy) {
    return CompletableFuture.supplyAsync(() -> {
      int numberOfRetries = retryPolicy.getNumOfRetries();
      while (numberOfRetries != 0) {
        try {
          return supplier.get();
        } catch (RetryPolicyException rpe) {
          numberOfRetries--;
        }
      }
      throw new RuntimeException();
    });
  }
}
