package site.ycsb.db.etcd;

import com.alibaba.fastjson.JSONObject;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Operators offer several modifier for Operator.
 *
 * See {@code etcd/README.md} for details.
 */
public final class Operators {
  private static final Logger LOG = LoggerFactory.getLogger(Operators.class);
  private static final Random RND = new Random();

  private Operators() {}

  public static boolean getBooleanProperty(Properties properties, String key, boolean defaultValue) {
    return Boolean.parseBoolean(properties.getProperty(key, Boolean.toString(defaultValue)));
  }

  // the default behavior of get response from etcd `get` api
  public static GetResponse getGetResult(CompletableFuture<GetResponse> get, long timeout, TimeUnit timeUnit)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (timeout <= 0L) {
      return get.get();
    } else {
      return get.get(timeout, timeUnit);
    }
  }

  // the default behavior of get response from etcd `put` api
  public static PutResponse getPutResult(CompletableFuture<PutResponse> get, long timeout, TimeUnit timeUnit)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (timeout <= 0L) {
      return get.get();
    } else {
      return get.get(timeout, timeUnit);
    }
  }

  // the default behavior of get response from etcd `delete` api
  public static DeleteResponse getDeleteResult(CompletableFuture<DeleteResponse> get, long timeout, TimeUnit timeUnit)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (timeout <= 0L) {
      return get.get();
    } else {
      return get.get(timeout, timeUnit);
    }
  }

  // the default behavior of get response from etcd `txn` api
  public static TxnResponse getTxnResult(CompletableFuture<TxnResponse> get, long timeout, TimeUnit timeUnit)
      throws ExecutionException, InterruptedException, TimeoutException {
    if (timeout <= 0L) {
      return get.get();
    } else {
      return get.get(timeout, timeUnit);
    }
  }

  public static ByteSequence toBs(String s, Charset charset) {
    return ByteSequence.from(s, charset);
  }

  public static ByteSequence toBs(Map<String, ?> m, Charset charset) {
    JSONObject out = new JSONObject();
    m.forEach((k, v)->out.put(k, v.toString()));
    return ByteSequence.from(out.toString(), charset);
  }

  public static String bsToString(ByteSequence bs, Charset charset) {
    return bs.toString(charset);
  }

  public static Map<String, String> bsToMap(ByteSequence bs, Charset charset) {
    JSONObject in = JSONObject.parseObject(bs.toString(charset));
    Map<String, String> res = new HashMap<>();
    in.forEach((k, v)->res.put(k, v.toString()));
    return res;
  }

  public static void getFieldValueResults(Map<String, String> values,
                                          Set<String> fields,
                                          Map<String, ByteIterator> results) {
    if (results != null) {
      if (fields == null) {
        results.putAll(StringByteIterator.getByteIteratorMap(values));
      } else {
        for (String field : fields) {
          String value = values.get(field);
          results.put(field, new StringByteIterator(value));
        }
      }

    }
  }

  public static Operator printActionOperator(Operator o) {
    return new Operator() {
      private final Operator nestedOp = o;
      private final long id = RND.nextLong();

      private String toString(Map<String, ByteIterator> values) {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        values.forEach((k, v) -> sj.add(String.format("%s=%s", k, v.toString())));
        return sj.toString();
      }

      @Override
      public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
          throws InterruptedException, ExecutionException, TimeoutException {
        Status s = nestedOp.read(table, key, fields, result);
        LOG.info("{} READ [table={},key={},res={}] [{}]", id, table, key, toString(result), s.toString());
        return s;
      }

      @Override
      public Status insert(String tableName, String key, Map<String, ByteIterator> values)
          throws InterruptedException, ExecutionException, TimeoutException {
        Status s = nestedOp.insert(tableName, key, values);
        LOG.info("{} INSERT [table={},key={},val={}] [{}]", id, tableName, key, toString(values), s.toString());
        return s;
      }

      @Override
      public Status update(String tableName, String key, Map<String, ByteIterator> values)
          throws InterruptedException, ExecutionException, TimeoutException {
        Status s = nestedOp.update(tableName, key, values);
        LOG.info("{} UPDATE [table={},key={},change_val={}] [{}]", id, tableName, key, toString(values), s.toString());
        return s;
      }

      @Override
      public Status delete(String tableName, String key)
          throws InterruptedException, ExecutionException, TimeoutException {
        Status s = nestedOp.delete(tableName, key);
        LOG.info("{} DELETE [table={},key={}] [{}]", id, tableName, key, s.toString());
        return s;
      }

      @Override
      public void cleanup() {}
    };
  }

  public static Operator errorInterceptedOperator(Operator o) {
    return new Operator() {
      private final Operator nestedOp = o;
      private final Map<String, Integer> stat = new HashMap<>();
      private final Map<String, Set<String>> errStat = new HashMap<>();

      private void recordStatus(Status s) {
        if (s != null) {
          stat.compute(s.getName(), (k, v) -> v == null ? 1 : v + 1);
        } else {
          stat.compute("null", (k, v) -> v == null ? 1 : v + 1);
        }
      }

      private String printErrStat() {
        if (errStat.isEmpty()) {
          return "";
        } else {
          StringJoiner sj = new StringJoiner(",", "[", "]");
          errStat.forEach((k, s) -> {
              StringJoiner ssj = new StringJoiner(",", k+"=[", "]");
              s.forEach(ssj::add);
              sj.add(ssj.toString());
            }
          );
          return sj.toString();
        }
      }

      private void recordException(Exception e) {
        stat.compute(Status.ERROR.getName(), (k, v) -> v == null ? 1 : v + 1);
        errStat.compute(e.toString(), (k, v) -> {
            if (v == null) {
              Set<String> set = new HashSet<>();
              set.add(e.getMessage());
              return set;
            } else {
              v.add(e.getMessage());
              return v;
            }
          }
        );
      }

      @Override
      public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        try {
          recordStatus(nestedOp.read(table, key, fields, result));
        } catch (Exception e) {
          recordException(e);
        }

        return Status.OK;
      }

      @Override
      public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
        try {
          recordStatus(nestedOp.insert(tableName, key, values));
        } catch (Exception e) {
          recordException(e);
        }

        return Status.OK;
      }

      @Override
      public Status update(String tableName, String key, Map<String, ByteIterator> values) {
        try {
          recordStatus(nestedOp.update(tableName, key, values));
        } catch (Exception e){
          recordException(e);
        }

        return Status.OK;
      }

      @Override
      public Status delete(String tableName, String key) {
        try {
          recordStatus(nestedOp.delete(tableName, key));
        } catch (Exception e) {
          recordException(e);
        }

        return Status.OK;
      }

      @Override
      public void cleanup() {
        StringJoiner sj = new StringJoiner(",", "[", "]");
        stat.forEach((k, v) -> sj.add(String.format("%s=%d", k, v)));
        LOG.info("ErrorInterceptedOperator: {} {}", sj, printErrStat());
      }
    };
  }
}
