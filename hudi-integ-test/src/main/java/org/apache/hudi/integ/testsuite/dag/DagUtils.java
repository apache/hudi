/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.integ.testsuite.dag;

import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayOutputStream;
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

import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.CONFIG_NAME;
import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.HIVE_PROPERTIES;
import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.HIVE_QUERIES;
import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.NO_DEPENDENCY_VALUE;
import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.PRESTO_PROPERTIES;
import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.PRESTO_QUERIES;
import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.TRINO_PROPERTIES;
import static org.apache.hudi.integ.testsuite.configuration.DeltaConfig.Config.TRINO_QUERIES;

/**
 * Utility class to SerDe workflow dag.
 */
public class DagUtils {

  public static final String DAG_NAME = "dag_name";
  public static final String DAG_ROUNDS = "dag_rounds";
  public static final String DAG_INTERMITTENT_DELAY_MINS = "dag_intermittent_delay_mins";
  public static final String DAG_CONTENT = "dag_content";

  public static int DEFAULT_DAG_ROUNDS = 1;
  public static int DEFAULT_INTERMITTENT_DELAY_MINS = 10;
  public static String DEFAULT_DAG_NAME = "TestDagName";

  static final ObjectMapper MAPPER = new ObjectMapper();

  /**
   * Converts a YAML path to {@link WorkflowDag}.
   */
  public static WorkflowDag convertYamlPathToDag(FileSystem fs, String path) throws IOException {
    InputStream is = fs.open(new Path(path));
    return convertYamlToDag(toString(is));
  }

  /**
   * Converts a YAML representation to {@link WorkflowDag}.
   */
  public static WorkflowDag convertYamlToDag(String yaml) throws IOException {
    int dagRounds = DEFAULT_DAG_ROUNDS;
    int intermittentDelayMins = DEFAULT_INTERMITTENT_DELAY_MINS;
    String dagName = DEFAULT_DAG_NAME;
    Map<String, DagNode> allNodes = new HashMap<>();
    final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    final JsonNode jsonNode = yamlReader.readTree(yaml);
    Iterator<Entry<String, JsonNode>> itr = jsonNode.fields();
    while (itr.hasNext()) {
      Entry<String, JsonNode> dagNode = itr.next();
      String key = dagNode.getKey();
      switch (key) {
        case DAG_NAME:
          dagName = dagNode.getValue().asText();
          break;
        case DAG_ROUNDS:
          dagRounds = dagNode.getValue().asInt();
          break;
        case DAG_INTERMITTENT_DELAY_MINS:
          intermittentDelayMins = dagNode.getValue().asInt();
          break;
        case DAG_CONTENT:
          JsonNode dagContent = dagNode.getValue();
          Iterator<Entry<String, JsonNode>> contentItr = dagContent.fields();
          while (contentItr.hasNext()) {
            Entry<String, JsonNode> dagContentNode = contentItr.next();
            allNodes.put(dagContentNode.getKey(), convertJsonToDagNode(allNodes, dagContentNode.getKey(), dagContentNode.getValue()));
          }
          break;
        default:
          break;
      }
    }
    return new WorkflowDag(dagName, dagRounds, intermittentDelayMins, findRootNodes(allNodes));
  }

  /**
   * Converts {@link WorkflowDag} to a YAML representation.
   */
  public static String convertDagToYaml(WorkflowDag dag) throws IOException {
    final ObjectMapper yamlWriter = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER)
        .enable(Feature.MINIMIZE_QUOTES).enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES));
    JsonNode yamlNode = MAPPER.createObjectNode();
    ((ObjectNode) yamlNode).put(DAG_NAME, dag.getDagName());
    ((ObjectNode) yamlNode).put(DAG_ROUNDS, dag.getRounds());
    ((ObjectNode) yamlNode).put(DAG_INTERMITTENT_DELAY_MINS, dag.getIntermittentDelayMins());
    JsonNode dagContentNode = MAPPER.createObjectNode();
    convertDagToYaml(dagContentNode, dag.getNodeList());
    ((ObjectNode) yamlNode).put(DAG_CONTENT, dagContentNode);
    return yamlWriter.writerWithDefaultPrettyPrinter().writeValueAsString(yamlNode);
  }

  private static void convertDagToYaml(JsonNode yamlNode, List<DagNode> dagNodes) throws IOException {
    for (DagNode dagNode : dagNodes) {
      String name = dagNode.getConfig().getOtherConfigs().getOrDefault(DeltaConfig.Config.NODE_NAME, dagNode.getName()).toString();
      ((ObjectNode) yamlNode).put(name, convertDagNodeToJsonNode(dagNode));
      if (dagNode.getChildNodes().size() > 0) {
        convertDagToYaml(yamlNode, dagNode.getChildNodes());
      }
    }
  }

  private static DagNode convertJsonToDagNode(Map<String, DagNode> allNodes, String name, JsonNode node)
      throws IOException {
    String type = node.get(DeltaConfig.Config.TYPE).asText();
    final DagNode retNode = convertJsonToDagNode(node, type, name);
    Arrays.asList(node.get(DeltaConfig.Config.DEPENDENCIES).textValue().split(",")).stream().forEach(dep -> {
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

  private static DagNode convertJsonToDagNode(JsonNode node, String type, String name) {
    try {
      DeltaConfig.Config config = DeltaConfig.Config.newBuilder().withConfigsMap(convertJsonNodeToMap(node))
          .withName(name).build();
      return (DagNode) ReflectionUtils.loadClass(generateFQN(type), config);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  private static String generateFQN(String name) throws ClassNotFoundException {
    return Class.forName(StringUtils.joinUsingDelim(".",
        DagNode.class.getName().substring(0, DagNode.class.getName().lastIndexOf(".")), name)).getName();
  }

  private static JsonNode convertDagNodeToJsonNode(DagNode node) throws IOException {
    return createJsonNode(node, node.getClass().getSimpleName());
  }

  private static Map<String, Object> convertJsonNodeToMap(JsonNode node) {
    Map<String, Object> configsMap = new HashMap<>();
    Iterator<Entry<String, JsonNode>> itr = node.get(CONFIG_NAME).fields();
    while (itr.hasNext()) {
      Entry<String, JsonNode> entry = itr.next();
      switch (entry.getKey()) {
        case HIVE_QUERIES:
          configsMap.put(HIVE_QUERIES, getQueries(entry));
          break;
        case HIVE_PROPERTIES:
          configsMap.put(HIVE_PROPERTIES, getQuerySessionProperties(entry));
          break;
        case PRESTO_QUERIES:
          configsMap.put(PRESTO_QUERIES, getQueries(entry));
          break;
        case PRESTO_PROPERTIES:
          configsMap.put(PRESTO_PROPERTIES, getQuerySessionProperties(entry));
          break;
        case TRINO_QUERIES:
          configsMap.put(TRINO_QUERIES, getQueries(entry));
          break;
        case TRINO_PROPERTIES:
          configsMap.put(TRINO_PROPERTIES, getQuerySessionProperties(entry));
          break;
        default:
          configsMap.put(entry.getKey(), getValue(entry.getValue()));
          break;
      }
    }
    return configsMap;
  }

  private static List<Pair<String, Integer>> getQueries(Entry<String, JsonNode> entry) {
    List<Pair<String, Integer>> queries = new ArrayList<>();
    try {
      List<JsonNode> flattened = new ArrayList<>();
      flattened.add(entry.getValue());
      queries = (List<Pair<String, Integer>>) getQueryMapper().readValue(flattened.toString(), List.class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return queries;
  }

  private static List<String> getQuerySessionProperties(Entry<String, JsonNode> entry) {
    List<String> properties = new ArrayList<>();
    try {
      List<JsonNode> flattened = new ArrayList<>();
      flattened.add(entry.getValue());
      properties = (List<String>) getQueryEnginePropertyMapper().readValue(flattened.toString(), List.class);
    } catch (Exception e) {
      e.printStackTrace();
    }
    return properties;
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

  private static JsonNode createJsonNode(DagNode node, String type) throws IOException {
    JsonNode configNode = MAPPER.readTree(node.getConfig().toString());
    JsonNode jsonNode = MAPPER.createObjectNode();
    Iterator<Entry<String, JsonNode>> itr = configNode.fields();
    while (itr.hasNext()) {
      Entry<String, JsonNode> entry = itr.next();
      switch (entry.getKey()) {
        case HIVE_QUERIES:
          ((ObjectNode) configNode).put(HIVE_QUERIES,
              MAPPER.readTree(getQueryMapper().writeValueAsString(node.getConfig().getHiveQueries())));
          break;
        case HIVE_PROPERTIES:
          ((ObjectNode) configNode).put(HIVE_PROPERTIES,
              MAPPER.readTree(getQueryEnginePropertyMapper().writeValueAsString(node.getConfig().getHiveProperties())));
          break;
        case PRESTO_QUERIES:
          ((ObjectNode) configNode).put(PRESTO_QUERIES,
              MAPPER.readTree(getQueryMapper().writeValueAsString(node.getConfig().getHiveQueries())));
          break;
        case PRESTO_PROPERTIES:
          ((ObjectNode) configNode).put(PRESTO_PROPERTIES,
              MAPPER.readTree(getQueryEnginePropertyMapper().writeValueAsString(node.getConfig().getHiveProperties())));
          break;
        case TRINO_QUERIES:
          ((ObjectNode) configNode).put(TRINO_QUERIES,
              MAPPER.readTree(getQueryMapper().writeValueAsString(node.getConfig().getHiveQueries())));
          break;
        case TRINO_PROPERTIES:
          ((ObjectNode) configNode).put(TRINO_PROPERTIES,
              MAPPER.readTree(getQueryEnginePropertyMapper().writeValueAsString(node.getConfig().getHiveProperties())));
          break;
        default:
          break;
      }
    }
    ((ObjectNode) jsonNode).put(CONFIG_NAME, configNode);
    ((ObjectNode) jsonNode).put(DeltaConfig.Config.TYPE, type);
    String dependencyNames = getDependencyNames(node);
    if (StringUtils.isNullOrEmpty(dependencyNames) || "\"\"".equals(dependencyNames)) {
      // Set "none" if there is no dependency
      dependencyNames = NO_DEPENDENCY_VALUE;
    }
    ((ObjectNode) jsonNode).put(DeltaConfig.Config.DEPENDENCIES, dependencyNames);
    return jsonNode;
  }

  private static String getDependencyNames(DagNode node) {
    return node.getParentNodes().stream()
        .map(e -> ((DagNode) e).getConfig().getOtherConfigs().getOrDefault(DeltaConfig.Config.NODE_NAME, node.getName()).toString())
        .collect(Collectors.joining(",")).toString();
  }

  public static String toString(InputStream inputStream) throws IOException {
    ByteArrayOutputStream result = new ByteArrayOutputStream();
    byte[] buffer = new byte[1024];
    int length;
    while ((length = inputStream.read(buffer)) != -1) {
      result.write(buffer, 0, length);
    }
    return result.toString("utf-8");
  }

  private static ObjectMapper getQueryMapper() {
    SimpleModule module = new SimpleModule();
    ObjectMapper queryMapper = new ObjectMapper();
    module.addSerializer(List.class, new QuerySerializer());
    module.addDeserializer(List.class, new QueryDeserializer());
    queryMapper.registerModule(module);
    return queryMapper;
  }

  private static final class QuerySerializer extends JsonSerializer<List> {
    Integer index = 0;

    @Override
    public void serialize(List pairs, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeStartObject();
      for (Pair pair : (List<Pair>) pairs) {
        gen.writeStringField("query" + index, pair.getLeft().toString());
        gen.writeNumberField("result" + index, Integer.parseInt(pair.getRight().toString()));
        index++;
      }
      gen.writeEndObject();
    }
  }

  private static final class QueryDeserializer extends JsonDeserializer<List> {
    @Override
    public List deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      List<Pair<String, Integer>> pairs = new ArrayList<>();
      String query = "";
      Integer result = 0;
      // [{query0:<query>, result0:<result>,query1:<query>, result1:<result>}]
      while (!parser.isClosed()) {
        JsonToken jsonToken = parser.nextToken();
        if (jsonToken.equals(JsonToken.END_ARRAY)) {
          break;
        }
        if (JsonToken.FIELD_NAME.equals(jsonToken)) {
          String fieldName = parser.getCurrentName();
          parser.nextToken();

          if (fieldName.contains("query")) {
            query = parser.getValueAsString();
          } else if (fieldName.contains("result")) {
            result = parser.getValueAsInt();
            pairs.add(Pair.of(query, result));
          }
        }
      }
      return pairs;
    }
  }

  private static ObjectMapper getQueryEnginePropertyMapper() {
    SimpleModule module = new SimpleModule();
    ObjectMapper propMapper = new ObjectMapper();
    module.addSerializer(List.class, new QueryEnginePropertySerializer());
    module.addDeserializer(List.class, new QueryEnginePropertyDeserializer());
    propMapper.registerModule(module);
    return propMapper;
  }

  private static final class QueryEnginePropertySerializer extends JsonSerializer<List> {
    Integer index = 0;

    @Override
    public void serialize(List props, JsonGenerator gen, SerializerProvider serializers) throws IOException {
      gen.writeStartObject();
      for (String prop : (List<String>) props) {
        gen.writeStringField("prop" + index, prop);
        index++;
      }
      gen.writeEndObject();
    }
  }

  private static final class QueryEnginePropertyDeserializer extends JsonDeserializer<List> {
    @Override
    public List deserialize(JsonParser parser, DeserializationContext context) throws IOException {
      List<String> props = new ArrayList<>();
      String prop = "";
      // [{prop0:<property>,...}]
      while (!parser.isClosed()) {
        JsonToken jsonToken = parser.nextToken();
        if (jsonToken.equals(JsonToken.END_ARRAY)) {
          break;
        }
        if (JsonToken.FIELD_NAME.equals(jsonToken)) {
          String fieldName = parser.getCurrentName();
          parser.nextToken();

          if (parser.getCurrentName().contains("prop")) {
            prop = parser.getValueAsString();
            props.add(prop);
          }
        }
      }
      return props;
    }
  }
}
