package com.uber.hoodie.bench.dag;

import static com.uber.hoodie.bench.configuration.DeltaConfig.Config.CONFIG_NAME;
import static com.uber.hoodie.bench.configuration.DeltaConfig.Config.DEPENDENCIES;
import static com.uber.hoodie.bench.configuration.DeltaConfig.Config.HIVE_QUERIES;
import static com.uber.hoodie.bench.configuration.DeltaConfig.Config.NODE_NAME;
import static com.uber.hoodie.bench.configuration.DeltaConfig.Config.TYPE;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import com.uber.hoodie.bench.configuration.DeltaConfig.Config;
import com.uber.hoodie.bench.dag.nodes.BulkInsertNode;
import com.uber.hoodie.bench.dag.nodes.DagNode;
import com.uber.hoodie.bench.dag.nodes.HiveQueryNode;
import com.uber.hoodie.bench.dag.nodes.HiveSyncNode;
import com.uber.hoodie.bench.dag.nodes.InsertNode;
import com.uber.hoodie.bench.dag.nodes.RollbackNode;
import com.uber.hoodie.bench.dag.nodes.SparkSQLQueryNode;
import com.uber.hoodie.bench.dag.nodes.UpsertNode;
import com.uber.hoodie.bench.dag.nodes.ValidateNode;
import com.uber.hoodie.common.util.collection.Pair;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Utility class to SerDe workflow dag
 */
public class DagUtils {

  static final ObjectMapper mapper = new ObjectMapper();

  /**
   * Converts a YAML path to {@link WorkflowDag}
   */
  public static WorkflowDag convertYamlPathToDag(FileSystem fs, String path) throws IOException {
    InputStream is = fs.open(new Path(path));
    return convertYamlToDag(IOUtils.toString(is, "utf-8"));
  }

  /**
   * Converts a YAML representation to {@link WorkflowDag}
   */
  public static WorkflowDag convertYamlToDag(String yaml) throws IOException {
    Map<String, DagNode> allNodes = new HashMap<>();
    final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    final JsonNode jsonNode = yamlReader.readTree(yaml);
    Iterator<Map.Entry<String, JsonNode>> itr = jsonNode.fields();
    while (itr.hasNext()) {
      Map.Entry<String, JsonNode> dagNode = itr.next();
      allNodes.put(dagNode.getKey(), convertJsonToDagNode(allNodes, dagNode.getValue()));
    }
    return new WorkflowDag(findRootNodes(allNodes));
  }

  /**
   * Converts {@link WorkflowDag} to a YAML representation
   */
  public static String convertDagToYaml(WorkflowDag dag) throws IOException {
    final ObjectMapper yamlWriter = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER)
        .enable(Feature.MINIMIZE_QUOTES).enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES));
    JsonNode yamlNode = mapper.createObjectNode();
    convertDagToYaml(yamlNode, dag.getNodeList());
    return yamlWriter.writerWithDefaultPrettyPrinter().writeValueAsString(yamlNode);
  }

  private static void convertDagToYaml(JsonNode yamlNode, List<DagNode> dagNodes) throws IOException {
    for (DagNode dagNode : dagNodes) {
      String name = dagNode.getConfig().getOtherConfigs().getOrDefault(NODE_NAME, dagNode.getName()).toString();
      ((ObjectNode) yamlNode).put(name, convertDagNodeToJsonNode(dagNode));
      if (dagNode.getChildNodes().size() > 0) {
        convertDagToYaml(yamlNode, dagNode.getChildNodes());
      }
    }
  }

  private static DagNode convertJsonToDagNode(Map<String, DagNode> allNodes, JsonNode node) throws IOException {
    String type = node.get(TYPE).asText();
    final DagNode retNode = convertJsonToDagNode(node, type);
    Arrays.asList(node.get(DEPENDENCIES).textValue().split(",")).stream().forEach(dep -> {
      DagNode parentNode = allNodes.get(dep);
      if (parentNode != null) {
        parentNode.addChildNode(retNode);
      }
    });
    return retNode;
  }

  private static List<DagNode> findRootNodes(Map<String, DagNode> allNodes) {
    final List<DagNode> rootNodes = new ArrayList<>();
    allNodes.entrySet().stream().forEach(entry -> {
      if (entry.getValue().getParentNodes().size() < 1) {
        rootNodes.add(entry.getValue());
      }
    });
    return rootNodes;
  }

  private static DagNode convertJsonToDagNode(JsonNode node, String type) {
    if (type.equalsIgnoreCase(InsertNode.class.getSimpleName())) {
      return new InsertNode(Config.newBuilder().withConfigsMap(convertJsonNodeToMap(node)).build());
    } else if (type.equalsIgnoreCase(BulkInsertNode.class.getSimpleName())) {
      return new BulkInsertNode(Config.newBuilder().withConfigsMap(convertJsonNodeToMap(node)).build());
    } else if (type.equalsIgnoreCase(UpsertNode.class.getSimpleName())) {
      return new UpsertNode(Config.newBuilder().withConfigsMap(convertJsonNodeToMap(node)).build());
    } else if (type.equalsIgnoreCase(ValidateNode.class.getSimpleName())) {
      return new ValidateNode(Config.newBuilder().build(), null);
    } else if (type.equalsIgnoreCase(HiveQueryNode.class.getSimpleName())) {
      return new HiveQueryNode(Config.newBuilder().withConfigsMap((convertJsonNodeToMap(node))).build());
    } else if (type.equalsIgnoreCase(SparkSQLQueryNode.class.getSimpleName())) {
      return new SparkSQLQueryNode(Config.newBuilder().withConfigsMap((convertJsonNodeToMap(node))).build());
    } else if (type.equalsIgnoreCase(RollbackNode.class.getSimpleName())) {
      return new RollbackNode(Config.newBuilder().withConfigsMap((convertJsonNodeToMap(node))).build());
    } else if (type.equalsIgnoreCase(HiveSyncNode.class.getSimpleName())) {
      return new HiveSyncNode(Config.newBuilder().withConfigsMap((convertJsonNodeToMap(node))).build());
    }
    // TODO : add cases for all node types
    throw new IllegalArgumentException("Unknown dag node inserted => " + node);
  }

  private static Map<String, Object> convertJsonNodeToMap(JsonNode node) {
    Map<String, Object> configsMap = new HashMap<>();
    Iterator<Entry<String, JsonNode>> itr = node.get(CONFIG_NAME).fields();
    while (itr.hasNext()) {
      Entry<String, JsonNode> entry = itr.next();
      switch (entry.getKey()) {
        case HIVE_QUERIES:
          configsMap.put(HIVE_QUERIES, getHiveQueries(entry));
          break;
        default:
          configsMap.put(entry.getKey(), getValue(entry.getValue()));
          break;
        // add any new scope added under CONFIG
      }
    }
    return configsMap;
  }

  private static List<Pair<String, Integer>> getHiveQueries(Entry<String, JsonNode> entry) {
    List<Pair<String, Integer>> queries = new ArrayList<>();
    Iterator<Entry<String, JsonNode>> queriesItr = entry.getValue().fields();
    while (queriesItr.hasNext()) {
      queries.add(Pair.of(queriesItr.next().getValue().textValue(), queriesItr.next().getValue().asInt()));
    }
    return queries;
  }

  private static Object getValue(JsonNode node) {
    if (node.isInt()) {
      return node.asInt();
    } else if (node.isLong()) {
      return node.asLong();
    } else if (node.isShort()) {
      return node.asInt();
    } else if (node.isBoolean()) {
      return node.asBoolean();
    } else if (node.isDouble()) {
      return node.asDouble();
    } else if (node.isFloat()) {
      return node.asDouble();
    }
    return node.textValue();
  }

  private static JsonNode convertDagNodeToJsonNode(DagNode node) throws IOException {
    if (node instanceof InsertNode) {
      return createJsonNode(node, InsertNode.class.getSimpleName());
    } else if (node instanceof BulkInsertNode) {
      return createJsonNode(node, BulkInsertNode.class.getSimpleName());
    } else if (node instanceof UpsertNode) {
      return createJsonNode(node, UpsertNode.class.getSimpleName());
    } else if (node instanceof ValidateNode) {
      return createJsonNode(node, ValidateNode.class.getSimpleName());
    } else if (node instanceof HiveSyncNode) {
      return createJsonNode(node, HiveSyncNode.class.getSimpleName());
    } else if (node instanceof HiveQueryNode) {
      return createJsonNode(node, HiveQueryNode.class.getSimpleName());
    } else if (node instanceof SparkSQLQueryNode) {
      return createJsonNode(node, SparkSQLQueryNode.class.getSimpleName());
    } else if (node instanceof RollbackNode) {
      return createJsonNode(node, RollbackNode.class.getSimpleName());
    } // TODO : add cases for all node types
    throw new IllegalArgumentException("Unknown dag node inserted => " + node);
  }

  private static JsonNode createJsonNode(DagNode node, String type) throws IOException {
    JsonNode configNode = mapper.readTree(node.getConfig().toString());
    JsonNode jsonNode = mapper.createObjectNode();
    ((ObjectNode) jsonNode).put(CONFIG_NAME, configNode);
    ((ObjectNode) jsonNode).put(TYPE, type);
    ((ObjectNode) jsonNode).put(DEPENDENCIES, getDependencyNames(node));
    return jsonNode;
  }

  private static String getDependencyNames(DagNode node) {
    return node.getParentNodes().stream()
        .map(e -> ((DagNode) e).getConfig().getOtherConfigs().getOrDefault(NODE_NAME, node.getName()).toString())
        .collect(Collectors.joining(",")).toString();
  }

}
