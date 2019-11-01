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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator.Feature;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.integ.testsuite.configuration.DeltaConfig;
import org.apache.hudi.integ.testsuite.dag.nodes.DagNode;

/**
 * Utility class to SerDe workflow dag.
 */
public class DagUtils {

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
    Map<String, DagNode> allNodes = new HashMap<>();
    final ObjectMapper yamlReader = new ObjectMapper(new YAMLFactory());
    final JsonNode jsonNode = yamlReader.readTree(yaml);
    Iterator<Entry<String, JsonNode>> itr = jsonNode.fields();
    while (itr.hasNext()) {
      Entry<String, JsonNode> dagNode = itr.next();
      allNodes.put(dagNode.getKey(), convertJsonToDagNode(allNodes, dagNode.getValue()));
    }
    return new WorkflowDag(findRootNodes(allNodes));
  }

  /**
   * Converts {@link WorkflowDag} to a YAML representation.
   */
  public static String convertDagToYaml(WorkflowDag dag) throws IOException {
    final ObjectMapper yamlWriter = new ObjectMapper(new YAMLFactory().disable(Feature.WRITE_DOC_START_MARKER)
        .enable(Feature.MINIMIZE_QUOTES).enable(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES));
    JsonNode yamlNode = MAPPER.createObjectNode();
    convertDagToYaml(yamlNode, dag.getNodeList());
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

  private static DagNode convertJsonToDagNode(Map<String, DagNode> allNodes, JsonNode node) throws IOException {
    String type = node.get(DeltaConfig.Config.TYPE).asText();
    final DagNode retNode = convertJsonToDagNode(node, type);
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

  private static DagNode convertJsonToDagNode(JsonNode node, String type) {
    try {
      DeltaConfig.Config config = DeltaConfig.Config.newBuilder().withConfigsMap(convertJsonNodeToMap(node)).build();
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
    Iterator<Entry<String, JsonNode>> itr = node.get(DeltaConfig.Config.CONFIG_NAME).fields();
    while (itr.hasNext()) {
      Entry<String, JsonNode> entry = itr.next();
      switch (entry.getKey()) {
        case DeltaConfig.Config.HIVE_QUERIES:
          configsMap.put(DeltaConfig.Config.HIVE_QUERIES, getHiveQueries(entry));
          break;
        case DeltaConfig.Config.HIVE_PROPERTIES:
          configsMap.put(DeltaConfig.Config.HIVE_PROPERTIES, getProperties(entry));
          break;
        default:
          configsMap.put(entry.getKey(), getValue(entry.getValue()));
          break;
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

  private static List<String> getProperties(Entry<String, JsonNode> entry) {
    List<String> properties = new ArrayList<>();
    Iterator<Entry<String, JsonNode>> queriesItr = entry.getValue().fields();
    while (queriesItr.hasNext()) {
      properties.add(queriesItr.next().getValue().textValue());
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
    ((ObjectNode) jsonNode).put(DeltaConfig.Config.CONFIG_NAME, configNode);
    ((ObjectNode) jsonNode).put(DeltaConfig.Config.TYPE, type);
    ((ObjectNode) jsonNode).put(DeltaConfig.Config.DEPENDENCIES, getDependencyNames(node));
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

}
