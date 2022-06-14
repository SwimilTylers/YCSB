package site.ycsb.db.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.KeyValue;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import io.etcd.jetcd.kv.PutResponse;
import io.etcd.jetcd.kv.TxnResponse;
import io.etcd.jetcd.op.Cmp;
import io.etcd.jetcd.op.CmpTarget;
import io.etcd.jetcd.op.Op;
import io.etcd.jetcd.options.DeleteOption;
import io.etcd.jetcd.options.GetOption;
import io.etcd.jetcd.options.PutOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.Status;

import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.*;

import static site.ycsb.db.etcd.Operators.*;

/**
 * The CachedOperator is an implementation of Operator.
 *
 * See {@code etcd/README.md} for details.
 */
public class CachedOperator implements Operator {
  protected static class CacheEntry {
    protected long createRevision;
    protected long version;
    protected ByteSequence value;

    static CacheEntry ofKeyValue(KeyValue kv) {
      CacheEntry e = new CacheEntry();
      e.createRevision = kv.getCreateRevision();
      e.version = kv.getVersion();
      e.value = kv.getValue();

      return e;
    }

    static CacheEntry ofDerived(CacheEntry parent, ByteSequence value) {
      CacheEntry e = new CacheEntry();
      e.createRevision = parent.createRevision;
      e.version = parent.version+1;
      e.value = value;

      return e;
    }

    static boolean equals(CacheEntry a, CacheEntry b) {
      return a == b ||
          (a != null && b != null && a.createRevision == b.createRevision && a.version == b.version);
    }
  }

  protected interface UpdateStat {
    enum Action {
      GET,
      TXN,
      TXN_NOT_FOUND,
      TXN_CAS_SUCCEED,
      TXN_CAS_FAILED
    }

    void start();
    void finish();
    String report();
    void action(Action a);
  }

  protected KV client;
  protected long timeout;
  protected long preheatTimeout;
  protected TimeUnit timeUnit;
  protected Charset charset;
  protected Map<String, CacheEntry> cache;
  protected long maxKeyPerGet;

  protected UpdateStat updateStat;

  private PutOption insertOption = PutOption.DEFAULT;
  private DeleteOption deleteOption = DeleteOption.DEFAULT;

  private static final Map<String, CacheEntry> UN_PREHEATED_SHARED_CACHE = new ConcurrentHashMap<>();
  private static final Logger LOG = LoggerFactory.getLogger(CachedOperator.class);
  public static final String STAT_UPDATE_NONE = "none";
  public static final String STAT_UPDATE_CAS = "cas";
  public static final String STAT_UPDATE_ACTION = "action";
  public static final String STAT_UPDATE_SHELL_ACTION = "shell_action";
  public static final String CACHE_SHARED = "etcd.cachedOperator.shared";
  public static final String LOAD_LIMIT = "etcd.cachedOperator.preheat.limit";
  public static final String LOAD_TIMEOUT = "etcd.cachedOperator.preheat.timeout";
  public static final String PREV_KV = "etcd.cachedOperator.prevKV";
  public static final String STAT_UPDATE = "etcd.cachedOperator.statOnUpdate";
  public static final boolean DEFAULT_CACHE_SHARED = true;
  public static final long DEFAULT_LIMIT = 2000;
  public static final boolean DEFAULT_PREV_KV = false;
  public static final String DEFAULT_STAT_UPDATE = STAT_UPDATE_NONE;

  public static CachedOperator ofInstance(
      KV kv, long timeout, TimeUnit timeUnit, Charset charset, Properties properties){

    CachedOperator op = new CachedOperator();
    op.client = kv;
    op.timeout = timeout;
    op.preheatTimeout = Long.parseLong(properties.getProperty(LOAD_TIMEOUT, Long.toString(timeout)));
    op.timeUnit = timeUnit;
    op.charset = charset;
    op.maxKeyPerGet = Long.parseLong(properties.getProperty(LOAD_LIMIT, Long.toString(DEFAULT_LIMIT)));
    op.updateStat = op.getUpdateStat(properties.getProperty(STAT_UPDATE, DEFAULT_STAT_UPDATE));

    if (getBooleanProperty(properties, CACHE_SHARED, DEFAULT_CACHE_SHARED)) {
      op.cache = CachedOperator.UN_PREHEATED_SHARED_CACHE;
    } else {
      op.cache = new HashMap<>();
    }

    if (getBooleanProperty(properties, PREV_KV, DEFAULT_PREV_KV)) {
      op.changeOptions(
          PutOption.newBuilder().withPrevKV().build(),
          DeleteOption.newBuilder().withPrevKV(true).build()
      );
    }

    return op;
  }

  protected UpdateStat getUpdateStat(String t) {
    switch (t) {
    case STAT_UPDATE_NONE:
      return new UpdateStat() {
        @Override
        public void start() {}

        @Override
        public void finish() {}

        @Override
        public String report() {
          return null;
        }

        @Override
        public void action(Action a) {}
      };
    case STAT_UPDATE_CAS:
      return new UpdateStat() {
        private long count = 0;
        private long thisCAS;
        private long totalCAS;

        @Override
        public void start() {
          thisCAS = 0;
        }

        @Override
        public void finish() {
          count++;
          totalCAS += thisCAS;
        }

        @Override
        public String report() {
          return String.format("totalCount=%d, averageCAS=%.2f", totalCAS, totalCAS * 1.0 / count);
        }

        @Override
        public void action(Action a) {
          if (a == Action.TXN_CAS_SUCCEED || a == Action.TXN_CAS_FAILED) {
            thisCAS++;
          }
        }
      };
    case STAT_UPDATE_ACTION:
      return new UpdateStat() {
        private long count = 0;
        private final long[] thisAction = new long[5];
        private final long[] totalAction = new long[5];

        @Override
        public void start() {
          Arrays.fill(thisAction, 0);
        }

        @Override
        public void finish() {
          count++;
          Arrays.setAll(totalAction, i -> totalAction[i] + thisAction[i]);
        }

        @Override
        public String report() {
          return String.format("total=%d, action[get/txn/txn-nf/txn-f/txn-s]=%s", count, Arrays.toString(totalAction));
        }

        @Override
        public void action(Action a) {
          switch (a) {
          case GET:
            thisAction[0]++;
            break;
          case TXN:
            thisAction[1]++;
            break;
          case TXN_NOT_FOUND:
            thisAction[2]++;
            break;
          case TXN_CAS_FAILED:
            thisAction[3]++;
            break;
          case TXN_CAS_SUCCEED:
            thisAction[4]++;
            break;
          default:
          }
        }
      };
    case STAT_UPDATE_SHELL_ACTION:
      return new UpdateStat() {
        @Override
        public void start() {}

        @Override
        public void finish() {}

        @Override
        public String report() {
          return null;
        }

        @Override
        public void action(Action a) {
          switch (a) {
            case GET:
              LOG.info("from [shell_action]: [M] key not found in cache");
              break;
            case TXN_NOT_FOUND:
              LOG.info("from [shell_action]: [X] value deleted");
              break;
            case TXN_CAS_FAILED:
              LOG.info("from [shell_action]: [F] value changed");
              break;
            case TXN_CAS_SUCCEED:
              LOG.info("from [shell_action]: [S] hit");
              break;
            default:
          }
        }
      };
    default:
      throw new IllegalArgumentException("unknown stat-update type: "+t);
    }
  }

  protected void updateCache(KeyValue kv) {
    cache.compute(bsToString(kv.getKey(), charset), (k, v) -> {
        if (v == null) {
          return CacheEntry.ofKeyValue(kv);
        } else {
          CacheEntry e = CacheEntry.ofKeyValue(kv);
          return e.createRevision == v.createRevision && e.version > v.version ? e : v;
        }
      }
    );
  }

  protected void updateCacheOnPrev(KeyValue prevKv, ByteSequence newValue) {
    CacheEntry e = CacheEntry.ofKeyValue(prevKv);
    String key = bsToString(prevKv.getKey(), charset);
    updateCacheOnOk(e, key, newValue);
  }

  protected void deleteCacheOnPrev(KeyValue prevKv, String key) {
    CacheEntry e = CacheEntry.ofKeyValue(prevKv);
    deleteCacheOnKeyNotFound(e, key);
  }

  protected void updateCacheOnOk(CacheEntry e, String key, ByteSequence newValue) {
    cache.compute(key, (k, v) -> {
        if (v == null) {
          return null;
        } else if (CacheEntry.equals(v, e)){
          return CacheEntry.ofDerived(v, newValue);
        } else {
          return v;
        }
      }
    );
  }

  protected void deleteCacheOnKeyNotFound(CacheEntry e, String key) {
    cache.compute(key, (k, v) -> {
        if (v == null || e.createRevision == v.createRevision) {
          return null;
        } else {
          return v;
        }
      }
    );
  }

  protected CacheEntry getCacheEntryOrNull(String key) {
    return cache.getOrDefault(key, null);
  }

  @Override
  public void cleanup() {
    String stat = updateStat.report();
    if (stat != null && stat.length() > 0) {
      LOG.warn("update statistics: {}", stat);
    }
  }

  protected void changeOptions(PutOption insert, DeleteOption delete) {
    this.insertOption = insert;
    this.deleteOption = delete;
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result)
      throws InterruptedException, ExecutionException, TimeoutException {

    GetResponse resp = getGetResult(this.client.get(toBs(key, charset)), timeout, timeUnit);
    if (resp.getCount() == 0) {
      return Status.NOT_FOUND;
    } else if (resp.getCount() > 1) {
      return Status.UNEXPECTED_STATE;
    } else {
      KeyValue kv = resp.getKvs().get(0);
      updateCache(kv);
      Map<String, String> fieldValues = bsToMap(kv.getValue(), charset);
      getFieldValueResults(fieldValues, fields, result);
      return Status.OK;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException {

    ByteSequence valSeq = toBs(values, charset);
    PutResponse resp = getPutResult(this.client.put(toBs(key, charset), valSeq, insertOption), timeout, timeUnit);
    if (resp.hasPrevKv()) {
      updateCacheOnPrev(resp.getPrevKv(), valSeq);
    }

    return Status.OK;
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values)
      throws InterruptedException, ExecutionException, TimeoutException {

    updateStat.start();
    CacheEntry entry = getCacheEntryOrNull(key);
    if (entry == null) {
      GetResponse getResp = getGetResult(this.client.get(toBs(key, charset)), timeout, timeUnit);
      updateStat.action(UpdateStat.Action.GET);
      if (getResp.getCount() == 0) {
        return Status.NOT_FOUND;
      } else if (getResp.getCount() > 1) {
        return Status.UNEXPECTED_STATE;
      }

      KeyValue kv = getResp.getKvs().get(0);
      updateCache(kv);
      entry = CacheEntry.ofKeyValue(kv);
    }

    while (true) {
      Map<String, String> oldValues = bsToMap(entry.value, charset);
      values.forEach((k, v) -> oldValues.put(k, v.toString()));
      ByteSequence valueSeq = toBs(oldValues, charset);

      ByteSequence keySeq = toBs(key, charset);
      TxnResponse txnResp = getTxnResult(
          this.client.txn().If(new Cmp(keySeq, Cmp.Op.GREATER, CmpTarget.createRevision(0)))
              .Then(Op.TxnOp.txn(
                  new Cmp[]{new Cmp(keySeq, Cmp.Op.EQUAL, CmpTarget.createRevision(entry.createRevision)),
                      new Cmp(keySeq, Cmp.Op.EQUAL, CmpTarget.version(entry.version))},
                  new Op[]{Op.put(keySeq, valueSeq, PutOption.DEFAULT)},
                  new Op[]{Op.get(keySeq, GetOption.DEFAULT)}))
              .commit(),
          timeout, timeUnit
      );

      updateStat.action(UpdateStat.Action.TXN);

      // key deleted when txn applied
      if (!txnResp.isSucceeded()) {
        updateStat.action(UpdateStat.Action.TXN_NOT_FOUND);
        updateStat.finish();
        deleteCacheOnKeyNotFound(entry, key);
        return Status.NOT_FOUND;
      }

      if (txnResp.getTxnResponses().size() != 1) {
        LOG.error("update txn response should be one, now size={}", txnResp.getTxnResponses().size());
        return Status.UNEXPECTED_STATE;
      }

      txnResp = txnResp.getTxnResponses().get(0);

      // entry is up-to-update, stop retry
      if (txnResp.isSucceeded()) {
        updateStat.action(UpdateStat.Action.TXN_CAS_SUCCEED);
        updateCacheOnOk(entry, key, valueSeq);
        break;
      }

      // entry is stale, refresh cache and retry to update
      if (txnResp.getGetResponses().size() != 1) {
        LOG.error("update txn-get response should be one, now size={}", txnResp.getGetResponses().size());
        return Status.UNEXPECTED_STATE;
      }

      GetResponse getResp = txnResp.getGetResponses().get(0);
      if (getResp.getCount() != 1) {
        return getResp.getCount() == 0 ? Status.NOT_FOUND : Status.UNEXPECTED_STATE;
      }

      KeyValue kv = getResp.getKvs().get(0);
      updateCache(kv);
      entry = CacheEntry.ofKeyValue(kv);

      updateStat.action(UpdateStat.Action.TXN_CAS_FAILED);
    }

    updateStat.finish();
    return Status.OK;
  }

  @Override
  public Status delete(String tableName, String key)
      throws InterruptedException, ExecutionException, TimeoutException {

    DeleteResponse resp = getDeleteResult(this.client.delete(toBs(key, charset), deleteOption), timeout, timeUnit);
    if (resp.getPrevKvs().size() == 1) {
      deleteCacheOnPrev(resp.getPrevKvs().get(0), key);
    }

    return resp.getDeleted() == 0 ? Status.NOT_FOUND : Status.OK;
  }

  @Override
  public void preheat(String table) throws InterruptedException, ExecutionException, TimeoutException {
    if (cache == UN_PREHEATED_SHARED_CACHE) {
      cache = CacheLoader.getInstance(client, preheatTimeout, timeUnit, charset, maxKeyPerGet).getCache();
      LOG.info("shared cache preheated, total key number = {}", cache.size());
    } else {
      cache = CacheLoader.getInstance(client, preheatTimeout, timeUnit, charset, maxKeyPerGet).getCopyOfCache();
      LOG.info("cache of {} preheated, total key number = {}", Thread.currentThread().getName(), cache.size());
    }
  }

  /**
   * The CacheLoader is a singleton for loading cache from etcd at preheat stage.
   *
   * See {@code etcd/README.md} for details.
   */
  public static final class CacheLoader {
    private static volatile CacheLoader cacheLoader;

    private final Map<String, CacheEntry> cache = new ConcurrentHashMap<>();

    private CacheLoader(){}

    public static CacheLoader getInstance(KV kv, long timeout, TimeUnit unit, Charset charset, long limit)
        throws InterruptedException, ExecutionException, TimeoutException {

      if (cacheLoader == null) {
        synchronized (CacheLoader.class) {
          if (cacheLoader == null) {
            CacheLoader loader = new CacheLoader();
            ByteSequence keyPrefixStart = ByteSequence.from(new byte[]{0});
            ByteSequence keyPrefixEnd = ByteSequence.from(new byte[]{0});
            loader.load(kv, timeout, unit, charset, limit, keyPrefixStart, keyPrefixEnd);

            cacheLoader = loader;
          }
        }
      }

      return cacheLoader;
    }

    public Map<String, CacheEntry> getCache() {
      return cache;
    }

    public Map<String, CacheEntry> getCopyOfCache() {
      return new HashMap<>(cache);
    }

    private void load(KV client, long timeout, TimeUnit timeUnit, Charset charset, long limitPerGet,
                      ByteSequence keyPrefixStart, ByteSequence keyPrefixEnd)
        throws ExecutionException, InterruptedException, TimeoutException {

      ByteSequence curKeyPrefixStart = keyPrefixStart;
      GetOption option = GetOption.newBuilder().withLimit(limitPerGet).withRange(keyPrefixEnd).build();

      GetResponse resp = getGetResult(client.get(curKeyPrefixStart, option), timeout, timeUnit);
      List<KeyValue> kvs = resp.getKvs();
      if (kvs.isEmpty()) {
        return;
      }
      kvs.forEach(kv -> cache.put(kv.getKey().toString(charset), CacheEntry.ofKeyValue(kv)));

      while (resp.isMore()) {
        curKeyPrefixStart = kvs.get(kvs.size()-1).getKey().concat(keyPrefixStart);
        resp = getGetResult(client.get(curKeyPrefixStart, option), timeout, timeUnit);
        kvs = resp.getKvs();
        if (kvs.isEmpty()) {
          break;
        }
        kvs.forEach(kv -> cache.put(kv.getKey().toString(charset), CacheEntry.ofKeyValue(kv)));
      }
    }
  }
}
