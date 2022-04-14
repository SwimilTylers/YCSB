package site.ycsb.db.etcd;

import com.alibaba.fastjson.JSONObject;
import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.ClientBuilder;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.cluster.Member;
import io.etcd.jetcd.kv.DeleteResponse;
import io.etcd.jetcd.kv.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import site.ycsb.*;

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
  private KV kv;
  private long actionTimeout;
  private Charset charset;

  public static final String ENDPOINTS = "etcd.endpoints";
  public static final String ACTION_TIMEOUT = "etcd.action.timeout";
  public static final String ACTION_PREHEAT = "etcd.action.preheat";
  public static final String CHARSET = "etcd.charset";
  public static final String USER_NAME = "etcd.user.name";
  public static final String PASSWORD = "etcd.user.password";
  public static final String NAMESPACE = "etcd.namespace";
  public static final String GET_CLUSTER_INFO = "etcd.getClusterInfo";

  public static final long DEFAULT_ACTION_TIMEOUT = 500;
  public static final boolean DEFAULT_ACTION_PREHEAT = true;
  public static final String DEFAULT_CHARSET = "UTF-8";
  public static final boolean DEFAULT_GET_CLUSTER_INFO = false;
  private final TimeUnit timeUnit = TimeUnit.MILLISECONDS;
  public static final String PREHEAT_PROBE = "__ycsb_preheat_probe";
  private static final Logger LOG = LoggerFactory.getLogger(EtcdClient.class);

  private ByteSequence keyToByteSequence(String s) {
    return ByteSequence.from(s, this.charset);
  }

  private ByteSequence valuesToByteSequence(Map<String, ?> values) {
    JSONObject out = new JSONObject();
    values.forEach((k, v)->out.put(k, v.toString()));
    LOG.debug("values to write {}", out);
    return ByteSequence.from(out.toString(), this.charset);
  }

  private Map<String, String> byteSequenceToValues(ByteSequence bs) {
    JSONObject in = JSONObject.parseObject(bs.toString(this.charset));
    Map<String, String> res = new HashMap<>();
    in.forEach((k, v)->res.put(k, v.toString()));
    LOG.debug("values to read {}", res);
    return res;
  }

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

    this.actionTimeout = Long.parseLong(prop.getProperty(ACTION_TIMEOUT, Long.toString(DEFAULT_ACTION_TIMEOUT)));
    this.charset = Charset.forName(prop.getProperty(CHARSET, DEFAULT_CHARSET));

    String user = prop.getProperty(USER_NAME);
    if (user != null && !user.isEmpty()) {
      builder.user(ByteSequence.from(user, this.charset));
      builder.password(ByteSequence.from(prop.getProperty(PASSWORD, ""), this.charset));
    }

    String namespace = prop.getProperty(NAMESPACE);
    if (namespace != null && !namespace.isEmpty()) {
      builder.namespace(ByteSequence.from(namespace, this.charset));
    }

    this.client = builder.build();

    if (Boolean.parseBoolean(prop.getProperty(GET_CLUSTER_INFO, Boolean.toString(DEFAULT_GET_CLUSTER_INFO)))){
      try {
        List<Member> mem = this.client.getClusterClient().
            listMember().
            get(this.actionTimeout, this.timeUnit).
            getMembers();
        StringJoiner sj = new StringJoiner(",", "[", "]");
        mem.forEach(m -> m.getClientURIs().forEach(uri -> sj.add(uri.toString())));
        LOG.info("Connect to cluster, size={}, available-endpoints={}", mem.size(), sj);
      } catch (Exception e) {
        throw new DBException("failed to get cluster info");
      }
    }

    this.kv = this.client.getKVClient();

    if (Boolean.parseBoolean(prop.getProperty(ACTION_PREHEAT, Boolean.toString(DEFAULT_ACTION_PREHEAT)))){
      try {
        this.kv.get(ByteSequence.from(PREHEAT_PROBE, this.charset)).get(this.actionTimeout, this.timeUnit);
      } catch (Exception e) {
        throw new DBException("failed to send preheat probe");
      }
    }
  }


  @Override
  public Status read(String tableName, String key, Set<String> fields, Map<String, ByteIterator> results) {
    try {
      GetResponse resp = this.kv.get(keyToByteSequence(key)).get(actionTimeout, timeUnit);
      if (resp.getCount() == 0) {
        return Status.NOT_FOUND;
      } else if (resp.getCount() > 1) {
        return Status.UNEXPECTED_STATE;
      } else {
        Map<String, String> fieldValues = byteSequenceToValues(resp.getKvs().get(0).getValue());

        if (results != null) {
          if (fields == null) {
            results.putAll(StringByteIterator.getByteIteratorMap(fieldValues));
          } else {
            for (String field : fields) {
              String value = fieldValues.get(field);
              results.put(field, new StringByteIterator(value));
            }
          }

        }

        return Status.OK;
      }
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
      GetResponse getResp = this.kv.get(keyToByteSequence(key)).get(actionTimeout, timeUnit);
      if (getResp.getCount() == 0) {
        return Status.NOT_FOUND;
      } else if (getResp.getCount() > 1) {
        return Status.UNEXPECTED_STATE;
      }

      Map<String, String> oldValues = byteSequenceToValues(getResp.getKvs().get(0).getValue());
      values.forEach((k, v)->oldValues.put(k, v.toString()));
      this.kv.put(keyToByteSequence(key), valuesToByteSequence(oldValues)).get(actionTimeout, timeUnit);

      return Status.OK;
    } catch (Exception e) {
      LOG.error("Error when updating a key:{}, tableName:{}", key, tableName, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      this.kv.put(keyToByteSequence(key), valuesToByteSequence(values)).get(actionTimeout, timeUnit);
      return Status.OK;
    } catch (Exception e) {
      LOG.error("Error when inserting a key:{}, tableName:{}", key, tableName, e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      DeleteResponse resp = this.kv.delete(keyToByteSequence(key)).get(actionTimeout, timeUnit);
      return resp.getDeleted() == 0 ? Status.NOT_FOUND : Status.OK;
    } catch (Exception e) {
      LOG.error("Error when deleting a key:{}, tableName:{}", key, tableName, e);
      return Status.ERROR;
    }
  }

  @Override
  public void cleanup() throws DBException {
    this.client.close();
    super.cleanup();
  }
}
