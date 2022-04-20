package site.ycsb.db.etcd;

import io.etcd.jetcd.test.EtcdClusterExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import site.ycsb.ByteIterator;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;
import site.ycsb.measurements.Measurements;
import site.ycsb.workloads.CoreWorkload;

import java.util.*;

import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY;
import static site.ycsb.workloads.CoreWorkload.TABLENAME_PROPERTY_DEFAULT;

class EtcdClientTest {
  @RegisterExtension
  public static final EtcdClusterExtension cluster = new EtcdClusterExtension(UUID.randomUUID().toString(), 1);

  private EtcdClient client;
  private String tableName;
  private Random rnd;

  protected static void setOperator(Class<? extends Operator> clazz, Properties prop) {
    if (clazz == SimpleOperator.class) {
      prop.setProperty(EtcdClient.ACTION_OPERATOR, EtcdClient.SIMPLE_OPERATOR);
    } else if (clazz == DeltaOperator.class) {
      prop.setProperty(EtcdClient.ACTION_OPERATOR, EtcdClient.DELTA_OPERATOR);
    } else if (clazz == CachedOperator.OnGetPutDeleteOperator.class) {
      prop.setProperty(EtcdClient.ACTION_OPERATOR, EtcdClient.CACHED_OPERATOR);
      prop.setProperty(CachedOperator.OP_TYPE, CachedOperator.TYPE_ON_GPD);
    } else {
      throw new UnsupportedOperationException();
    }
  }

  @BeforeEach
  void setUp() throws Exception {
    client = new EtcdClient();

    Properties p = new Properties();
    StringJoiner joiner = new StringJoiner(",");
    cluster.getClientEndpoints().forEach(url-> joiner.add(url.toString()));
    p.setProperty(EtcdClient.ENDPOINTS, joiner.toString());
    setOperator(CachedOperator.OnGetPutDeleteOperator.class, p);

    Measurements.setProperties(p);
    final CoreWorkload workload = new CoreWorkload();
    workload.init(p);

    tableName = p.getProperty(TABLENAME_PROPERTY, TABLENAME_PROPERTY_DEFAULT);
    rnd = new Random();

    client.setProperties(p);
    client.init();
  }

  @AfterEach
  void tearDown() throws Exception {
    client.cleanup();
  }

  @Test
  void testEtcdClient() {
    String testKey = "ycsb-test-key-"+rnd.nextLong();

    // insert
    Map<String, String> m = new HashMap<>();
    String field1 = "field_1";
    String value1 = "value_1";
    m.put(field1, value1);
    Map<String, ByteIterator> result = StringByteIterator.getByteIteratorMap(m);
    Status status = client.insert(tableName, testKey, result);
    Assertions.assertEquals(Status.OK, status);

    // read
    result.clear();
    status = client.read(tableName, testKey, null, result);
    Assertions.assertEquals(Status.OK, status);
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(value1, result.get(field1).toString());

    // update(the same field)
    m.clear();
    result.clear();
    String newVal = "value_new";
    m.put(field1, newVal);
    result = StringByteIterator.getByteIteratorMap(m);
    status = client.update(tableName, testKey, result);
    Assertions.assertEquals(Status.OK, status);
    Assertions.assertEquals(1, result.size());

    // Verify result
    result.clear();
    status = client.read(tableName, testKey, null, result);
    Assertions.assertEquals(Status.OK, status);
    // here we only have one field: field_1
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(newVal, result.get(field1).toString());

    // update(two different field)
    m.clear();
    result.clear();
    String field2 = "field_2";
    String value2 = "value_2";
    m.put(field2, value2);
    result = StringByteIterator.getByteIteratorMap(m);
    status = client.update(tableName, testKey, result);
    Assertions.assertEquals(Status.OK, status);
    Assertions.assertEquals(1, result.size());

    // Verify result
    result.clear();
    status = client.read(tableName, testKey, null, result);
    Assertions.assertEquals(Status.OK, status);
    // here we have two field: field_1 and field_2
    Assertions.assertEquals(2, result.size());
    Assertions.assertEquals(newVal, result.get(field1).toString());
    Assertions.assertEquals(value2, result.get(field2).toString());

    // delete
    status = client.delete(tableName, testKey);
    Assertions.assertEquals(Status.OK, status);

    // Verify result
    result.clear();
    status = client.read(tableName, testKey, null, result);
    Assertions.assertEquals(Status.NOT_FOUND, status);
    Assertions.assertEquals(0, result.size());

    // insert again
    m = new HashMap<>();
    String field3 = "field_3";
    String value3 = "value_3";
    m.put(field3, value3);
    result = StringByteIterator.getByteIteratorMap(m);
    status = client.insert(tableName, testKey, result);
    Assertions.assertEquals(Status.OK, status);

    // Verify result
    result.clear();
    status = client.read(tableName, testKey, null, result);
    Assertions.assertEquals(Status.OK, status);
    Assertions.assertEquals(1, result.size());
    Assertions.assertEquals(value3, result.get(field3).toString());
  }
}