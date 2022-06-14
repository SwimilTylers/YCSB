package site.ycsb.db.etcd;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.cluster.Member;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * The YCSB binding for <a href="https://etcd.io/">Etcd</a>.
 *
 * See {@code etcd/README.md} for details.
 */
public class EtcdClient extends DB {
  private Client client;
  private Operator op;

  public static final String SIMPLE_OPERATOR = "simpleOperator";
  public static final String DELTA_OPERATOR = "deltaOperator";
  public static final String CACHED_OPERATOR = "cachedOperator";
  public static final String ENDPOINTS = "etcd.endpoints";
  public static final String ACTION_TIMEOUT = "etcd.action.timeout";
  public static final String ACTION_PREHEAT = "etcd.action.preheat";
  public static final String ACTION_OPERATOR = "etcd.action.operator";
  public static final String OP_SUPPRESS_EX = "etcd.action.suppressExceptions";
  public static final String OP_PRINT_EACH = "etcd.action.printEach";
  public static final String CHARSET = "etcd.charset";
  public static final String USER_NAME = "etcd.user.name";
  public static final String PASSWORD = "etcd.user.password";
  public static final String NAMESPACE = "etcd.namespace";
  public static final String GET_CLUSTER_INFO = "etcd.getClusterInfo";

  public static final long DEFAULT_ACTION_TIMEOUT = 500;
  public static final boolean DEFAULT_ACTION_PREHEAT = true;
  public static final String DEFAULT_ACTION_OPERATOR = CACHED_OPERATOR;

  public static final boolean DEFAULT_OP_SUPPRESS_EX = false;
  public static final boolean DEFAULT_OP_PRINT_EACH = false;
  public static final String DEFAULT_CHARSET = "UTF-8";
  public static final boolean DEFAULT_GET_CLUSTER_INFO = false;
  private final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
  private static final Logger LOG = LoggerFactory.getLogger(EtcdClient.class);

  @Override
  public void init() throws DBException {
    super.init();

    ClientBuilder builder = Client.builder();
    Properties prop = getProperties();

    String endpoints = prop.getProperty(ENDPOINTS);
    if (endpoints == null || endpoints.isEmpty()) {
      throw new DBException("etcd.endpoints must not be empty");
    }

    List<URI> endpointUrls = new ArrayList<>();
    for (String url : endpoints.split(",")) {
      if (url != null && !url.isEmpty()) {
        try {
          if (!url.contains("://")) {
            url = "http://" + url;
          }
          endpointUrls.add(new URI(url));
        } catch (URISyntaxException e) {
          throw new DBException("illegal url found in etcd.endpoints: "+url);
        }
      }
    }

    if (endpointUrls.isEmpty()) {
      throw new DBException("no valid url found in etcd.endpoints");
    }

    builder.endpoints(endpointUrls);

    long actionTimeout = Long.parseLong(prop.getProperty(ACTION_TIMEOUT, Long.toString(DEFAULT_ACTION_TIMEOUT)));
    Charset charset = Charset.forName(prop.getProperty(CHARSET, DEFAULT_CHARSET));

    String user = prop.getProperty(USER_NAME);
    if (user != null && !user.isEmpty()) {
      builder.user(ByteSequence.from(user, charset));
      builder.password(ByteSequence.from(prop.getProperty(PASSWORD, ""), charset));
    }

    String namespace = prop.getProperty(NAMESPACE);
    if (namespace != null && !namespace.isEmpty()) {
      builder.namespace(ByteSequence.from(namespace, charset));
    }

    this.client = builder.build();

    if (Boolean.parseBoolean(prop.getProperty(GET_CLUSTER_INFO, Boolean.toString(DEFAULT_GET_CLUSTER_INFO)))){
      try {
        List<Member> mem;
        if (actionTimeout <= 0L) {
          mem = this.client.getClusterClient().
              listMember().
              get().
              getMembers();
        } else {
          mem = this.client.getClusterClient().
              listMember().
              get(actionTimeout, this.timeUnit).
              getMembers();
        }
        StringJoiner sj = new StringJoiner(",", "[", "]");
        mem.forEach(m -> m.getClientURIs().forEach(uri -> sj.add(uri.toString())));
        LOG.info("Connect to cluster, size={}, available-endpoints={}", mem.size(), sj);
      } catch (Exception e) {
        throw new DBException("failed to get cluster info");
      }
    }

    KV kv = this.client.getKVClient();

    switch (prop.getProperty(ACTION_OPERATOR, DEFAULT_ACTION_OPERATOR)) {
    case SIMPLE_OPERATOR:
      this.op = SimpleOperator.ofInstance(kv, actionTimeout, timeUnit, charset);
      break;
    case DELTA_OPERATOR:
      this.op = DeltaOperator.ofInstance(kv, actionTimeout, timeUnit, charset, prop);
      break;
    case CACHED_OPERATOR:
      this.op = CachedOperator.ofInstance(kv, actionTimeout, timeUnit, charset, prop);
      break;
    default:
      throw new DBException("unknown operator");
    }

    if (Boolean.parseBoolean(prop.getProperty(ACTION_PREHEAT, Boolean.toString(DEFAULT_ACTION_PREHEAT)))){
      try {
        this.op.preheat("");
      } catch (Exception e) {
        LOG.error("an exception occur during the preheat", e);
        throw new DBException("failed to send preheat probe");
      }
    }

    if (Boolean.parseBoolean(prop.getProperty(OP_PRINT_EACH, Boolean.toString(DEFAULT_OP_PRINT_EACH)))) {
      this.op = Operators.printActionOperator(this.op);
    }

    if (Boolean.parseBoolean(prop.getProperty(OP_SUPPRESS_EX, Boolean.toString(DEFAULT_OP_SUPPRESS_EX)))) {
      this.op = Operators.errorInterceptedOperator(this.op);
    }
  }


  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> results) {
    try {
      return op.read(tableName, key, fields, results);
    }  catch (Exception e) {
      LOG.error("Error when reading a key:{}, tableName:{}", key, tableName, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName,
                     String startKey,
                     int recordCount,
                     Set<String> fields,
                     Vector<HashMap<String, ByteIterator>> results) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      return op.update(tableName, key, values);
    } catch (Exception e) {
      LOG.error("Error when updating a key:{}, tableName:{}", key, tableName, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      return op.insert(tableName, key, values);
    } catch (Exception e) {
      LOG.error("Error when inserting a key:{}, tableName:{}", key, tableName, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      return op.delete(tableName, key);
    } catch (Exception e) {
      LOG.error("Error when deleting a key:{}, tableName:{}", key, tableName, e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    this.op.cleanup();
    this.client.close();
    super.cleanup();
  }
}
