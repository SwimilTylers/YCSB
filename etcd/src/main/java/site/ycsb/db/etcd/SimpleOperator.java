package site.ycsb.db.etcd;

import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

import java.nio.charset.Charset;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The SimpleOperator is an implementation of Operator.
 *
 * See {@code etcd/README.md} for details.
 */
public class SimpleOperator implements Operator {
  private KV kv;
  private long timeout;
  private TimeUnit timeUnit;
  private Charset charset;

  public static SimpleOperator ofInstance(
      KV kv, long timeout, TimeUnit timeUnit, Charset charset) {
    SimpleOperator op = new SimpleOperator();

    op.kv = kv;
    op.timeout = timeout;
    op.timeUnit = timeUnit;
    op.charset = charset;

    return op;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
      throws InterruptedException, ExecutionException, TimeoutException {

    GetResponse resp = this.kv.get(toBs(key, charset)).get(timeout, timeUnit);
    if (resp.getCount() == 0) {
      return Status.NOT_FOUND;
    } else if (resp.getCount() > 1) {
      return Status.UNEXPECTED_STATE;
    } else {
      Map<String, String> fieldValues = bsToMap(resp.getKvs().get(0).getValue(), charset);
      getFieldValueResults(fieldValues, fields, result);
      return Status.OK;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException {

    this.kv.put(toBs(key, charset), toBs(values, charset)).get(timeout, timeUnit);
    return Status.OK;
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException {

    GetResponse getResp = this.kv.get(toBs(key, charset)).get(timeout, timeUnit);
    if (getResp.getCount() == 0) {
      return Status.NOT_FOUND;
    } else if (getResp.getCount() > 1) {
      return Status.UNEXPECTED_STATE;
    }

    Map<String, String> oldValues = bsToMap(getResp.getKvs().get(0).getValue(), charset);
    values.forEach((k, v)->oldValues.put(k, v.toString()));
    this.kv.put(toBs(key, charset), toBs(oldValues, charset)).get(timeout, timeUnit);

    return Status.OK;
  }

  @Override
  public Status delete(String tableName, String key)
      throws InterruptedException, ExecutionException, TimeoutException {

    DeleteResponse resp = this.kv.delete(toBs(key, charset)).get(timeout, timeUnit);
    return resp.getDeleted() == 0 ? Status.NOT_FOUND : Status.OK;
  }

  @Override
  public void cleanup() {}
}
