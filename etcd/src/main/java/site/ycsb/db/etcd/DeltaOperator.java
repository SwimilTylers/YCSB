package site.ycsb.db.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.OptionsUtil;
import io.etcd.jetcd.options.PutOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * The DeltaOperator is an implementation of Operator.
 *
 * See {@code etcd/README.md} for details.
 */
public class DeltaOperator implements Operator{
  private String name;
  private AtomicLong counter;
  private KV client;
  private long timeout;
  private TimeUnit timeUnit;
  private Charset charset;

  private String levelDelimiter;
  private String metaRoot;
  private String deltaRoot;

  public static final String OPERATOR_NAME = "etcd.deltaOperator.name";
  public static final String LEVEL_DELIMITER = "etcd.deltaOperator.levelDelimiter";
  public static final String ROOT = "etcd.deltaOperator.root";
  public static final String META = "etcd.deltaOperator.meta";
  public static final String DELTA = "etcd.deltaOperator.delta";

  public static final String DEFAULT_LEVEL_DELIMITER = "/";
  public static final String DEFAULT_ROOT = "__ycsb";
  public static final String DEFAULT_META_DIR = "meta";
  public static final String DEFAULT_DELTA_DIR = "delta";

  private static final Logger LOG = LoggerFactory.getLogger(DeltaOperator.class);

  private static String randName() {
    int leftLimit = 48; // numeral '0'
    int rightLimit = 122; // letter 'z'
    int targetStringLength = 8;
    Random random = new Random();

    return random.ints(leftLimit, rightLimit + 1)
        .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
        .limit(targetStringLength)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
  }

  public static DeltaOperator ofInstance(
      KV kv, long timeout, TimeUnit timeUnit, Charset charset, Properties otherProperties) {

    String name = otherProperties.getProperty(OPERATOR_NAME, randName());
    return ofInstance(name, kv, timeout, timeUnit, charset, otherProperties);
  }

  public static DeltaOperator ofInstance(
      String name, KV kv, long timeout, TimeUnit timeUnit, Charset charset, Properties otherProperties) {

    DeltaOperator op = new DeltaOperator();

    op.name = name;
    op.counter = new AtomicLong(0);
    op.client = kv;
    op.timeout = timeout;
    op.timeUnit = timeUnit;
    op.charset = charset;

    op.levelDelimiter = otherProperties.getProperty(LEVEL_DELIMITER, DEFAULT_LEVEL_DELIMITER);
    String root = otherProperties.getProperty(ROOT, DEFAULT_ROOT);
    op.metaRoot = root+op.levelDelimiter+otherProperties.getProperty(META, DEFAULT_META_DIR);
    op.deltaRoot = root+op.levelDelimiter+otherProperties.getProperty(DELTA, DEFAULT_DELTA_DIR);

    return op;
  }

  private String metaKey(String key) {
    return metaRoot+levelDelimiter+key;
  }

  private String deltaKeysPrefix(String key) {
    return deltaRoot+levelDelimiter+key+levelDelimiter;
  }

  private String starterDeltaKey(String key, String starter) {
    return deltaKeysPrefix(key)+starter;
  }

  private String opStarter() {
    return name+levelDelimiter+counter.incrementAndGet();
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
      throws InterruptedException, ExecutionException, TimeoutException {

    ByteSequence deltaKeysStart = toBs(deltaKeysPrefix(key), charset);
    ByteSequence deltaKeysEnd = OptionsUtil.prefixEndOf(deltaKeysStart);

    List<GetResponse> gets = client.txn().Then(
        Op.get(toBs(metaKey(key), charset), GetOption.DEFAULT),
        Op.get(deltaKeysStart, GetOption.newBuilder()
            .withRange(deltaKeysEnd)
            .withSortOrder(GetOption.SortOrder.ASCEND)
            .withSortField(GetOption.SortTarget.MOD)
            .build()
        )
    ).commit().get(timeout, timeUnit).getGetResponses();

    if (gets.size() != 2) {
      return Status.UNEXPECTED_STATE;
    }

    GetResponse metaResp = gets.get(0);
    GetResponse deltaResp = gets.get(1);

    if (metaResp.getCount() != 1) {
      return metaResp.getCount() == 0 ? Status.NOT_FOUND : Status.UNEXPECTED_STATE;
    }

    ByteSequence starterKey = toBs(
        starterDeltaKey(key, bsToString(metaResp.getKvs().get(0).getValue(), charset)),
        charset
    );
    List<KeyValue> deltas = deltaResp.getKvs();
    Iterator<KeyValue> iter = deltas.listIterator();
    Map<String, String> fieldValues = null;

    // find starter value
    while (iter.hasNext()){
      KeyValue kv = iter.next();
      if (kv.getKey().equals(starterKey)) {
        fieldValues = bsToMap(kv.getValue(), charset);
        break;
      }
    }

    if (fieldValues == null) {
      return Status.UNEXPECTED_STATE;
    }

    // apply deltas
    while (iter.hasNext()) {
      KeyValue kv = iter.next();
      fieldValues.putAll(bsToMap(kv.getValue(), charset));
    }

    getFieldValueResults(fieldValues, fields, result);

    return Status.OK;
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException {

    ByteSequence metaKeySeq = toBs(metaKey(key), charset);
    String starter = opStarter();

    client.txn().Then(
            Op.put(metaKeySeq, toBs(starter, charset), PutOption.DEFAULT),
            Op.put(toBs(starterDeltaKey(key, starter), charset), toBs(values, charset), PutOption.DEFAULT)
        )
        .commit().get(timeout, timeUnit);

    LOG.debug("write {}={}, {}={}", metaKeySeq, starter, starterDeltaKey(key, starter), toBs(values, charset));

    return Status.OK;
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException {

    ByteSequence metaKeySeq = toBs(metaKey(key), charset);
    String starter = opStarter();

    TxnResponse resp = client.txn().If(new Cmp(metaKeySeq, Cmp.Op.GREATER, CmpTarget.createRevision(0)))
        .Then(Op.put(toBs(starterDeltaKey(key, starter), charset), toBs(values, charset), PutOption.DEFAULT))
        .commit().get(timeout, timeUnit);

    return resp.isSucceeded() ? Status.OK : Status.NOT_FOUND;
  }

  @Override
  public Status delete(String tableName, String key)
      throws InterruptedException, ExecutionException, TimeoutException {

    ByteSequence metaKeySeq = toBs(metaKey(key), charset);

    TxnResponse resp = client.txn().If(new Cmp(metaKeySeq, Cmp.Op.GREATER, CmpTarget.createRevision(0)))
        .Then(
            Op.delete(metaKeySeq, DeleteOption.DEFAULT),
            Op.delete(toBs(deltaKeysPrefix(key), charset), DeleteOption.newBuilder().isPrefix(true).build())
        )
        .commit().get(timeout, timeUnit);

    return resp.isSucceeded() ? Status.OK : Status.NOT_FOUND;
  }

  @Override
  public void cleanup() {}
}
