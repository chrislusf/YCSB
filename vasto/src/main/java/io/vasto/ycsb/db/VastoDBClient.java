package io.vasto.ycsb.db;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.Status;
import com.yahoo.ycsb.StringByteIterator;
import io.grpc.Context;
import io.vasto.ClusterClient;
import io.vasto.KeyObject;
import io.vasto.OpAndDataType;
import io.vasto.ValueObject;
import io.vasto.VastoClient;
import org.apache.log4j.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Vector;

/**
 * Vasto client for YCSB.
 */

public class VastoDBClient extends DB {

  private static final Logger LOGGER = Logger.getLogger(VastoDBClient.class);
  protected static final ObjectMapper MAPPER = new ObjectMapper();

  private ClusterClient clusterClient;

  private String masterHost = "localhost";
  private int masterPort = 8278;
  private int maxConnects = 50;

  @Override
  public void init() throws DBException {

    String masterHostValue = getProperties().getProperty("vasto.masterHost", masterHost);
    String masterPortValue = getProperties().getProperty("vasto.masterPort", null);
    String keyspace = getProperties().getProperty("vasto.keyspace", "ks1");
    String dataCenter = getProperties().getProperty("vasto.datacenter", "dc1");

    if (null != masterPortValue) {
      this.masterPort = Integer.parseInt(masterPortValue);
    }

    if (null != masterHostValue) {
      this.masterHost = masterHostValue;
    }

    VastoClient client = new VastoClient(Context.current(),
        this.masterHost, this.masterPort, dataCenter, "ycsb_bench");

    this.clusterClient = client.newClusterClient(keyspace);

    LOGGER.info("vasto connection created with " + this.masterHost);
  }

  @Override
  public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {

    KeyObject k = new KeyObject(key.getBytes());

    try {
      ValueObject valueObject = clusterClient.get(k);
      if (valueObject != null) {
        fromJson(new String(valueObject.getValue().toByteArray()), fields, result);
      }
    } catch (Exception e) {
      LOGGER.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status scan(String table, String startkey, int recordcount,
                     Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status update(String table, String key, Map<String, ByteIterator> values) {
    return Status.NOT_IMPLEMENTED;
  }

  @Override
  public Status insert(String table, String key, Map<String, ByteIterator> values) {

    KeyObject k = new KeyObject(key.getBytes());

    try {
      this.clusterClient.put(k, new ValueObject(toJson(values).getBytes(), OpAndDataType.BYTES));
    } catch (Exception e) {
      LOGGER.error("Error encountered for key: " + key, e);
      return Status.ERROR;
    }

    return Status.OK;
  }

  @Override
  public Status delete(String table, String key) {
    return Status.NOT_IMPLEMENTED;
  }

  protected static void fromJson(
      String value, Set<String> fields,
      Map<String, ByteIterator> result) throws IOException {
    JsonNode json = MAPPER.readTree(value);
    boolean checkFields = fields != null && !fields.isEmpty();
    for (Iterator<Entry<String, JsonNode>> jsonFields = json.getFields();
         jsonFields.hasNext();
      /* increment in loop body */) {
      Map.Entry<String, JsonNode> jsonField = jsonFields.next();
      String name = jsonField.getKey();
      if (checkFields && fields.contains(name)) {
        continue;
      }
      JsonNode jsonValue = jsonField.getValue();
      if (jsonValue != null && !jsonValue.isNull()) {
        result.put(name, new StringByteIterator(jsonValue.asText()));
      }
    }
  }

  protected static String toJson(Map<String, ByteIterator> values)
      throws IOException {
    ObjectNode node = MAPPER.createObjectNode();
    Map<String, String> stringMap = StringByteIterator.getStringMap(values);
    for (Map.Entry<String, String> pair : stringMap.entrySet()) {
      node.put(pair.getKey(), pair.getValue());
    }
    JsonFactory jsonFactory = new JsonFactory();
    Writer writer = new StringWriter();
    JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(writer);
    MAPPER.writeTree(jsonGenerator, node);
    return writer.toString();
  }

}
