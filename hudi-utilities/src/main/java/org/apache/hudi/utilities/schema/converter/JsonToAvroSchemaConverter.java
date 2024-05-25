/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.utilities.schema.converter;

import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.util.StringUtils.getSuffixBy;
import static org.apache.hudi.common.util.StringUtils.removeSuffixBy;

public class JsonToAvroSchemaConverter implements SchemaRegistryProvider.SchemaConverter {

  private static final ObjectMapper MAPPER = JsonUtils.getObjectMapper();
  private static final Map<String, String> JSON_TO_AVRO_TYPE = Stream.of(new String[][] {
      {"string", "string"},
      {"null", "null"},
      {"boolean", "boolean"},
      {"integer", "long"},
      {"number", "double"}
  }).collect(Collectors.collectingAndThen(Collectors.toMap(p -> p[0], p -> p[1]), Collections::<String, String>unmodifiableMap));
  private static final Pattern SYMBOL_REGEX = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");

  @Override
  public String convert(String jsonSchema) throws IOException {
    JsonNode jsonNode = MAPPER.readTree(jsonSchema);
    ObjectNode avroRecord = MAPPER.createObjectNode()
        .put("type", "record")
        .put("name", getAvroSchemaRecordName(jsonNode))
        .put("doc", getAvroDoc(jsonNode));
    Option<String> namespace = getAvroSchemaRecordNamespace(jsonNode);
    if (namespace.isPresent()) {
      avroRecord.put("namespace", namespace.get());
    }
    if (jsonNode.hasNonNull("properties")) {
      avroRecord.set("fields", convertProperties(jsonNode.get("properties"), getRequired(jsonNode)));
    } else {
      avroRecord.set("fields", MAPPER.createArrayNode());
    }
    return avroRecord.toString();
  }

  private static ArrayNode convertProperties(JsonNode jsonProperties, Set<String> required) {
    List<JsonNode> avroFields = new ArrayList<>(jsonProperties.size());
    jsonProperties.fieldNames().forEachRemaining(name ->
        avroFields.add(tryConvertNestedProperty(name, jsonProperties.get(name))
            .or(() -> tryConvertArrayProperty(name, jsonProperties.get(name)))
            .or(() -> tryConvertEnumProperty(name, jsonProperties.get(name)))
            .orElseGet(() -> convertProperty(name, jsonProperties.get(name), required.contains(name)))));
    return MAPPER.createArrayNode().addAll(avroFields);
  }

  private static Option<JsonNode> tryConvertNestedProperty(String name, JsonNode jsonProperty) {
    if (!isJsonNestedType(jsonProperty)) {
      return Option.empty();
    }
    JsonNode avroNode = MAPPER.createObjectNode()
        .put("name", sanitizeAsAvroName(name))
        .put("doc", getAvroDoc(jsonProperty))
        .set("type", MAPPER.createObjectNode()
            .put("type", "record")
            .put("name", getAvroTypeName(jsonProperty, name))
            .set("fields", convertProperties(jsonProperty.get("properties"), getRequired(jsonProperty))));

    return Option.of(avroNode);
  }

  private static Option<JsonNode> tryConvertArrayProperty(String name, JsonNode jsonProperty) {
    if (!isJsonArrayType(jsonProperty)) {
      return Option.empty();
    }
    JsonNode avroItems;
    JsonNode jsonItems = jsonProperty.get("items");
    String itemName = getAvroTypeName(jsonItems, name) + "_child";
    if (isJsonNestedType(jsonItems)) {
      avroItems = MAPPER.createObjectNode()
          .put("type", "record")
          .put("name", itemName)
          .set("fields", convertProperties(jsonItems.get("properties"), getRequired(jsonItems)));
    } else {
      avroItems = convertProperty(itemName, jsonItems, true);
    }
    JsonNode avroNode = MAPPER.createObjectNode()
        .put("name", sanitizeAsAvroName(name))
        .put("doc", getAvroDoc(jsonProperty))
        .set("type", MAPPER.createObjectNode()
            .put("type", "array")
            .set("items", avroItems));

    return Option.of(avroNode);
  }

  private static Option<JsonNode> tryConvertEnumProperty(String name, JsonNode jsonProperty) {
    if (!isJsonEnumType(jsonProperty)) {
      return Option.empty();
    }
    List<String> enums = new ArrayList<>();
    jsonProperty.get("enum").iterator().forEachRemaining(e -> enums.add(e.asText()));
    JsonNode avroType = enums.stream().allMatch(e -> SYMBOL_REGEX.matcher(e).matches())
        ? MAPPER.createObjectNode()
        .put("type", "enum")
        .put("name", getAvroTypeName(jsonProperty, name))
        .set("symbols", jsonProperty.get("enum"))
        : TextNode.valueOf("string");
    JsonNode avroNode = MAPPER.createObjectNode()
        .put("name", sanitizeAsAvroName(name))
        .put("doc", getAvroDoc(jsonProperty))
        .set("type", avroType);

    return Option.of(avroNode);
  }

  private static JsonNode convertProperty(String name, JsonNode jsonProperty, boolean isRequired) {
    ObjectNode avroNode = MAPPER.createObjectNode()
        .put("name", sanitizeAsAvroName(name))
        .put("doc", getAvroDoc(jsonProperty));

    // infer `default`
    boolean nullable = !isRequired;
    if (jsonProperty.has("default")) {
      avroNode.set("default", jsonProperty.get("default"));
    } else if (nullable) {
      avroNode.set("default", NullNode.getInstance());
    }

    // infer `types`
    Set<String> avroTypeSet = new HashSet<>();
    if (jsonProperty.hasNonNull("oneOf") || jsonProperty.hasNonNull("allOf")) {
      // prefer to look for `oneOf` and `allOf` for types
      Option<JsonNode> oneOfTypes = Option.ofNullable(jsonProperty.get("oneOf"));
      if (oneOfTypes.isPresent()) {
        oneOfTypes.get().elements().forEachRemaining(e -> avroTypeSet.add(JSON_TO_AVRO_TYPE.get(e.get("type").asText())));
      }
      Option<JsonNode> allOfTypes = Option.ofNullable(jsonProperty.get("allOf"));
      if (allOfTypes.isPresent()) {
        allOfTypes.get().elements().forEachRemaining(e -> avroTypeSet.add(JSON_TO_AVRO_TYPE.get(e.get("type").asText())));
      }
    } else if (jsonProperty.has("type")) {
      // fall back to `type` parameter
      JsonNode jsonType = jsonProperty.get("type");
      if (jsonType.isArray()) {
        jsonType.elements().forEachRemaining(e -> avroTypeSet.add(JSON_TO_AVRO_TYPE.get(e.asText())));
      } else {
        avroTypeSet.add(JSON_TO_AVRO_TYPE.get(jsonType.asText()));
      }
    }
    List<String> avroTypes = new ArrayList<>();
    if (nullable || avroTypeSet.contains("null")) {
      avroTypes.add("null");
    }
    avroTypeSet.remove("null");
    avroTypes.addAll(avroTypeSet);
    avroNode.set("type", avroTypes.size() > 1
        ? MAPPER.createArrayNode().addAll(avroTypes.stream().map(TextNode::valueOf).collect(Collectors.toList()))
        : TextNode.valueOf(avroTypes.get(0)));
    return avroNode;
  }

  private static boolean isJsonNestedType(JsonNode jsonNode) {
    return jsonNode.has("type") && Objects.equals(jsonNode.get("type").asText(), "object");
  }

  private static boolean isJsonArrayType(JsonNode jsonNode) {
    return jsonNode.has("type") && Objects.equals(jsonNode.get("type").asText(), "array");
  }

  private static boolean isJsonEnumType(JsonNode jsonNode) {
    return jsonNode.hasNonNull("enum") && jsonNode.get("enum").isArray();
  }

  private static Option<String> getAvroSchemaRecordNamespace(JsonNode jsonNode) {
    if (jsonNode.hasNonNull("$id")) {
      String host = URI.create(jsonNode.get("$id").asText()).getHost();
      String avroNamespace = Stream.of(host.split("\\."))
          .map(JsonToAvroSchemaConverter::sanitizeAsAvroName)
          .collect(Collectors.joining("."));
      return Option.of(avroNamespace);
    }
    return Option.empty();
  }

  private static String getAvroSchemaRecordName(JsonNode jsonNode) {
    if (jsonNode.hasNonNull("title")) {
      return sanitizeAsAvroName(jsonNode.get("title").asText());
    }
    if (jsonNode.hasNonNull("$id")) {
      // infer name from host: http://www.my-example.com => "my_example"
      String host = URI.create(jsonNode.get("$id").asText()).getHost();
      String domain = removeSuffixBy(host, '.');
      return sanitizeAsAvroName(getSuffixBy(domain, '.'));
    }
    // avro schema requires non-empty record name
    return "no_name";
  }

  private static String sanitizeAsAvroName(String s) {
    return s.replaceAll("[^A-Za-z0-9_]+", "_");
  }

  private static Set<String> getRequired(JsonNode jsonNode) {
    if (!jsonNode.hasNonNull("required")) {
      return Collections.emptySet();
    }
    JsonNode requiredNode = jsonNode.get("required");
    Set<String> required = new HashSet<>(requiredNode.size());
    jsonNode.get("required").elements().forEachRemaining(e -> required.add(e.asText()));
    return required;
  }

  private static String getAvroTypeName(JsonNode jsonNode, String defaultName) {
    return jsonNode.hasNonNull("title") ? jsonNode.get("title").asText() : defaultName;
  }

  private static String getAvroDoc(JsonNode jsonNode) {
    return jsonNode.hasNonNull("description") ? jsonNode.get("description").asText() : "";
  }
}
