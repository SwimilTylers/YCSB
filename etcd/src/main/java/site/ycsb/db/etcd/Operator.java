package site.ycsb.db.etcd;

import com.alibaba.fastjson.JSONObject;
import io.etcd.jetcd.ByteSequence;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.nio.charset.Charset;
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
  Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
      throws InterruptedException, ExecutionException, TimeoutException;
  Status insert(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException;
  Status update(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException;
  Status delete(String tableName, String key) throws InterruptedException, ExecutionException, TimeoutException;
  void cleanup();

  default ByteSequence toBs(String s, Charset charset) {
    return ByteSequence.from(s, charset);
  }

  default ByteSequence toBs(Map<String, ?> m, Charset charset) {
    JSONObject out = new JSONObject();
    m.forEach((k, v)->out.put(k, v.toString()));
    return ByteSequence.from(out.toString(), charset);
  }

  default String bsToString(ByteSequence bs, Charset charset) {
    return bs.toString(charset);
  }

  default Map<String, String> bsToMap(ByteSequence bs, Charset charset) {
    JSONObject in = JSONObject.parseObject(bs.toString(charset));
    Map<String, String> res = new HashMap<>();
    in.forEach((k, v)->res.put(k, v.toString()));
    return res;
  }

  default void getFieldValueResults(Map<String, String> values, Set<String> fields, Map<String, ByteIterator> results) {
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
}
