package site.ycsb.db.etcd;

import site.ycsb.ByteIterator;
import site.ycsb.Status;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

/**
 * The etcd operator interface for EtcdClient.
 *
 * See {@code etcd/README.md} for details.
 */
public interface Operator {
  String PREHEAT_PROBE = "__ycsb_preheat_probe";

  default void preheat(String table) throws InterruptedException, ExecutionException, TimeoutException {
    read(table, PREHEAT_PROBE, null, new HashMap<>());
  }
  Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
      throws InterruptedException, ExecutionException, TimeoutException;
  Status insert(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException;
  Status update(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException;
  Status delete(String tableName, String key) throws InterruptedException, ExecutionException, TimeoutException;
  void cleanup();
}
