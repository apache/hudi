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

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.util.JsonUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ConfigUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.utilities.schema.SchemaRegistryProvider;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import io.confluent.kafka.schemaregistry.ParsedSchema;
import io.confluent.kafka.schemaregistry.json.JsonSchema;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverterConfig.CONVERT_DEFAULT_VALUE_TYPE;
import static org.apache.hudi.utilities.schema.converter.JsonToAvroSchemaConverterConfig.STRIP_DEFAULT_VALUE_QUOTES;

/**
 * Converts a JSON schema to an Avro schema.
 */
public class JsonToAvroSchemaConverter implements SchemaRegistryProvider.SchemaConverter {
  private static final ObjectMapper MAPPER = JsonUtils.getObjectMapper();
  private static final Map<String, String> JSON_TO_AVRO_TYPE =
      Stream.of(
              new String[][] {
                  {"string", "string"},
                  {"null", "null"},
                  {"boolean", "boolean"},
                  {"integer", "long"},
                  {"number", "double"}
              })
          .collect(
              Collectors.collectingAndThen(
                  Collectors.toMap(p -> p[0], p -> p[1]),
                  Collections::<String, String>unmodifiableMap));
  private static final Pattern SYMBOL_REGEX = Pattern.compile("^[A-Za-z_][A-Za-z0-9_]*$");
  private static final String LONG_TYPE = "long";
  private static final String DOUBLE_TYPE = "double";
  private static final String BOOLEAN_TYPE = "boolean";
  private static final String STRING_TYPE = "string";
  private static final String DEFAULT_FIELD = "default";

  private final boolean convertDefaultValueType;
  private final boolean stripDefaultValueQuotes;

  public JsonToAvroSchemaConverter(TypedProperties config) {
    this.convertDefaultValueType = ConfigUtils.getBooleanWithAltKeys(config, CONVERT_DEFAULT_VALUE_TYPE);
    this.stripDefaultValueQuotes = ConfigUtils.getBooleanWithAltKeys(config, STRIP_DEFAULT_VALUE_QUOTES);
  }

  public String convert(ParsedSchema parsedSchema) throws IOException {
    JsonSchema jsonSchema = (JsonSchema) parsedSchema;
    JsonNode jsonNode = MAPPER.readTree(jsonSchema.canonicalString());
    ObjectNode avroRecord = convertJsonNodeToAvroNode(jsonNode, new AtomicInteger(1), new HashSet<>());
    return avroRecord.toString();
  }

  private ObjectNode convertJsonNodeToAvroNode(JsonNode jsonNode, AtomicInteger schemaCounter, Set<String> seenNames) {
    ObjectNode avroRecord =
        MAPPER
            .createObjectNode()
            .put("type", "record")
            .put("name", getAvroSchemaRecordName(jsonNode))
            .put("doc", getAvroDoc(jsonNode));
    Option<String> namespace = getAvroSchemaRecordNamespace(jsonNode);
    if (namespace.isPresent()) {
      avroRecord.put("namespace", namespace.get());
    }
    if (jsonNode.hasNonNull("properties")) {
      avroRecord.set(
          "fields", convertProperties(jsonNode.get("properties"), getRequired(jsonNode), schemaCounter, seenNames));
    } else {
      avroRecord.set("fields", MAPPER.createArrayNode());
    }
    return avroRecord;
  }

  private ArrayNode convertProperties(JsonNode jsonProperties, Set<String> required, AtomicInteger schemaCounter,
                                      Set<String> seenNames) {
    List<JsonNode> avroFields = new ArrayList<>();
    jsonProperties
        .fieldNames()
        .forEachRemaining(
            name -> {
              avroFields.add(
                  tryConvertNestedProperty(name, jsonProperties.get(name), schemaCounter, seenNames)
                      .or(() -> tryConvertArrayProperty(name, jsonProperties.get(name), schemaCounter, seenNames))
                      .or(() -> tryConvertEnumProperty(name, jsonProperties.get(name), schemaCounter, seenNames))
                      .orElseGet(() ->
                          convertProperty(
                              name, jsonProperties.get(name), required.contains(name), schemaCounter, seenNames, false)));
            });

    return MAPPER.createArrayNode().addAll(avroFields);
  }

  private Option<JsonNode> tryConvertNestedProperty(String name, JsonNode jsonProperty,
                                                    AtomicInteger schemaCounter, Set<String> seenNames) {
    if (!isJsonNestedType(jsonProperty)) {
      return Option.empty();
    }
    JsonNode avroNode =
        MAPPER
            .createObjectNode()
            .put("name", sanitizeAsAvroName(name))
            .put("doc", getAvroDoc(jsonProperty))
            .set(
                "type",
                MAPPER
                    .createObjectNode()
                    .put("type", "record")
                    .put("name", getAvroTypeName(jsonProperty, name, schemaCounter, seenNames))
                    .set(
                        "fields",
                        convertProperties(
                            jsonProperty.get("properties"), getRequired(jsonProperty), schemaCounter, seenNames)));

    return Option.of(avroNode);
  }

  private Option<JsonNode> tryConvertArrayProperty(String name, JsonNode jsonProperty,
                                                   AtomicInteger schemaCounter, Set<String> seenNames) {
    if (!isJsonArrayType(jsonProperty)) {
      return Option.empty();
    }
    JsonNode avroItems;
    JsonNode jsonItems = jsonProperty.get("items");
    String itemName = getAvroTypeName(jsonItems, name, schemaCounter, seenNames) + "_child";
    if (isJsonNestedType(jsonItems)) {
      avroItems =
          MAPPER
              .createObjectNode()
              .put("type", "record")
              .put("name", itemName)
              .set(
                  "fields", convertProperties(jsonItems.get("properties"), getRequired(jsonItems),
                              schemaCounter, seenNames));
    } else {
      avroItems = convertProperty(itemName, jsonItems, true, schemaCounter, seenNames, true);
    }
    JsonNode avroNode =
        MAPPER
            .createObjectNode()
            .put("name", sanitizeAsAvroName(name))
            .put("doc", getAvroDoc(jsonProperty))
            .set("type", MAPPER.createObjectNode().put("type", "array").set("items", avroItems));

    return Option.of(avroNode);
  }

  private static Option<JsonNode> tryConvertEnumProperty(String name, JsonNode jsonProperty,
                                                         AtomicInteger schemaCounter, Set<String> seenNames) {
    if (!isJsonEnumType(jsonProperty)) {
      return Option.empty();
    }
    List<String> enums = new ArrayList<>();
    jsonProperty.get("enum").iterator().forEachRemaining(e -> enums.add(e.asText()));
    JsonNode avroType =
        enums.stream().allMatch(e -> SYMBOL_REGEX.matcher(e).matches())
            ? MAPPER
            .createObjectNode()
            .put("type", "enum")
            .put("name", getAvroTypeName(jsonProperty, name, schemaCounter, seenNames))
            .set("symbols", jsonProperty.get("enum"))
            : TextNode.valueOf("string");
    JsonNode avroNode =
        MAPPER
            .createObjectNode()
            .put("name", sanitizeAsAvroName(name))
            .put("doc", getAvroDoc(jsonProperty))
            .set("type", avroType);

    return Option.of(avroNode);
  }

  private JsonNode convertProperty(String name, JsonNode jsonProperty, boolean isRequired,
                                   AtomicInteger schemaCounter, Set<String> seenNames,
                                   boolean isArrayType) {
    ObjectNode avroNode =
        MAPPER
            .createObjectNode()
            .put("name", sanitizeAsAvroName(name))
            .put("doc", getAvroDoc(jsonProperty));

    boolean nullable = !isRequired;

    // infer `types`
    Set<String> avroSimpleTypeSet = new HashSet<>();
    List<JsonNode> defaultValueList = new ArrayList<>();
    List<JsonNode> avroComplexTypeSet = new ArrayList<>();
    boolean unionType = false;
    if (jsonProperty.hasNonNull("oneOf") || jsonProperty.hasNonNull("allOf")) {
      unionType = true;
      // prefer to look for `oneOf` and `allOf` for types
      Option<JsonNode> oneOfTypes = Option.ofNullable(jsonProperty.get("oneOf"));
      Pair<Pair<Set<String>, List<JsonNode>>, List<JsonNode>> allOneOfTypes =
          getAllTypesFromOneOfAllOfTypes(oneOfTypes, name, schemaCounter, seenNames);
      avroSimpleTypeSet.addAll(allOneOfTypes.getLeft().getLeft());
      defaultValueList.addAll(allOneOfTypes.getLeft().getRight());
      avroComplexTypeSet.addAll(allOneOfTypes.getRight());

      Option<JsonNode> allOfTypes = Option.ofNullable(jsonProperty.get("allOf"));
      Pair<Pair<Set<String>, List<JsonNode>>, List<JsonNode>> allAllOfTypes =
          getAllTypesFromOneOfAllOfTypes(allOfTypes, name, schemaCounter, seenNames);
      avroSimpleTypeSet.addAll(allAllOfTypes.getLeft().getLeft());
      avroComplexTypeSet.addAll(allAllOfTypes.getRight());
    } else if (jsonProperty.has("type")) {
      // fall back to `type` parameter
      JsonNode jsonType = jsonProperty.get("type");
      if (jsonType.isArray()) {
        jsonType
            .elements()
            .forEachRemaining(e -> avroSimpleTypeSet.add(JSON_TO_AVRO_TYPE.get(e.asText())));
      } else {
        avroSimpleTypeSet.add(JSON_TO_AVRO_TYPE.get(jsonType.asText()));
      }
    }
    List<String> avroTypes = new ArrayList<>();
    if (nullable || avroSimpleTypeSet.contains("null")) {
      avroTypes.add("null");
    }
    avroSimpleTypeSet.remove("null");
    avroTypes.addAll(avroSimpleTypeSet);

    // infer `default`
    // Either there is explicit default value defined for a single simple type,
    // e.g., "field":{"type":"integer","default":50},
    // or we take the default value from the single simple type in "oneOf" type,
    // e.g., "oneOf":[{"type":"null"},{"type":"integer","default":"60"}],
    // in which the default value is 60.
    JsonNode defaultNode = jsonProperty.has(DEFAULT_FIELD) ? jsonProperty.get(DEFAULT_FIELD)
        : (avroComplexTypeSet.isEmpty() && avroSimpleTypeSet.size() == 1 && !defaultValueList.isEmpty())
        ? defaultValueList.get(0) : NullNode.getInstance();
    if (jsonProperty.has(DEFAULT_FIELD)
        || (avroComplexTypeSet.isEmpty() && avroSimpleTypeSet.size() == 1 && !defaultValueList.isEmpty())) {
      if (this.convertDefaultValueType && avroComplexTypeSet.isEmpty() && avroSimpleTypeSet.size() == 1) {
        String defaultType = avroSimpleTypeSet.stream().findFirst().get();
        switch (defaultType) {
          case LONG_TYPE:
            defaultNode = LongNode.valueOf(Long.parseLong(defaultNode.asText()));
            break;
          case DOUBLE_TYPE:
            defaultNode = DoubleNode.valueOf(Double.parseDouble(defaultNode.asText()));
            break;
          case BOOLEAN_TYPE:
            defaultNode = BooleanNode.valueOf(Boolean.parseBoolean(defaultNode.asText()));
            break;
          case STRING_TYPE:
            if (this.stripDefaultValueQuotes) {
              defaultNode = TextNode.valueOf(stripQuotesFromStringValue(defaultNode.asText()));
            }
            break;
          default:
        }
      }
      avroNode.set(DEFAULT_FIELD, defaultNode);
    } else if (nullable) {
      avroNode.set(DEFAULT_FIELD, defaultNode);
    }
    avroTypes = arrangeTypeOrderOnDefault(avroTypes, defaultNode);
    List<JsonNode> allTypes =
        avroTypes.stream().map(TextNode::valueOf).collect(Collectors.toList());
    allTypes.addAll(avroComplexTypeSet);
    ArrayNode typeValue = MAPPER.createArrayNode().addAll(allTypes);
    avroNode.set(
        "type", allTypes.size() > 1 ? typeValue : allTypes.get(0));
    return unionType && isArrayType ? typeValue : avroNode;
  }

  private Pair<Pair<Set<String>, List<JsonNode>>, List<JsonNode>> getAllTypesFromOneOfAllOfTypes(Option<JsonNode> jsonUnionType, String name, AtomicInteger schemaCounter, Set<String> seenNames) {
    Set<String> avroSimpleTypeSet = new HashSet<>();
    List<JsonNode> defaultValueList = new ArrayList<>();
    List<JsonNode> avroComplexTypeSet = new ArrayList<>();
    if (jsonUnionType.isPresent()) {
      jsonUnionType
          .get()
          .elements()
          .forEachRemaining(
              e -> {
                String simpleType = JSON_TO_AVRO_TYPE.get(e.get("type").asText());
                if (simpleType == null) {
                  if (isJsonNestedType(e)) {
                    avroComplexTypeSet.add(tryConvertNestedProperty(name, e, schemaCounter, seenNames).get().get("type"));
                  } else if (isJsonArrayType(e)) {
                    avroComplexTypeSet.add(tryConvertArrayProperty(name, e, schemaCounter, seenNames).get().get("type"));
                  } else if (isJsonEnumType(e)) {
                    avroComplexTypeSet.add(tryConvertEnumProperty(name, e, schemaCounter, seenNames).get().get("type"));
                  } else {
                    throw new RuntimeException("unknown complex type encountered");
                  }
                } else {
                  avroSimpleTypeSet.add(simpleType);
                  if (e.has(DEFAULT_FIELD)) {
                    // This is only valid if one simple-type exists in the oneOf type
                    defaultValueList.add(e.get(DEFAULT_FIELD));
                  }
                }
              });
    }
    return Pair.of(Pair.of(avroSimpleTypeSet, defaultValueList), avroComplexTypeSet);
  }

  private static List<String> arrangeTypeOrderOnDefault(List<String> avroTypes, JsonNode defaultNode) {
    // Nothing to be done as null is already the first one.
    if (defaultNode == null || defaultNode.isNull()) {
      return avroTypes;
    }
    Set<String> avroTypesSet = new HashSet<>(avroTypes);
    if (defaultNode.isInt() || defaultNode.isBigInteger() || defaultNode.isIntegralNumber()) {
      // Maps to Long Type in Avro. Place it first, defaulting to null if not found.
      return modifyListOrderingBasedOnDefaultValue(avroTypes, avroTypesSet, LONG_TYPE);
    } else if (defaultNode.isNumber() || defaultNode.isBigDecimal() || defaultNode.isDouble()
            || defaultNode.isFloatingPointNumber()) {
      return modifyListOrderingBasedOnDefaultValue(avroTypes, avroTypesSet, DOUBLE_TYPE);
    } else if (defaultNode.isTextual()) {
      return modifyListOrderingBasedOnDefaultValue(avroTypes, avroTypesSet, STRING_TYPE);
    } else if (defaultNode.isBoolean()) {
      return modifyListOrderingBasedOnDefaultValue(avroTypes, avroTypesSet, BOOLEAN_TYPE);
    }
    return avroTypes;
  }

  private static List<String> modifyListOrderingBasedOnDefaultValue(List<String> typeList,
                                                                    Set<String> avroTypesSet,
                                                                    String type) {
    List<String> modifiedAvroTypeList = new ArrayList<>();
    if (avroTypesSet.contains(type)) {
      modifiedAvroTypeList.add(type);
      avroTypesSet.remove(type);
      modifiedAvroTypeList.addAll(avroTypesSet);
      return modifiedAvroTypeList;
    }
    // Return original list.
    return typeList;
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
      String avroNamespace =
          Stream.of(host.split("\\."))
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

  private static String getAvroTypeName(JsonNode jsonNode, String defaultName,
                                        AtomicInteger schemaCounter,
                                        Set<String> seenNames) {
    String typeName = jsonNode.hasNonNull("title") ? jsonNode.get("title").asText()
            : defaultName;
    if (!seenNames.contains(typeName)) {
      seenNames.add(typeName);
      return typeName;
    }
    String modifiedTypeName = typeName + schemaCounter.getAndIncrement();
    seenNames.add(modifiedTypeName);
    return modifiedTypeName;
  }

  private static String getAvroDoc(JsonNode jsonNode) {
    return jsonNode.hasNonNull("description") ? jsonNode.get("description").asText() : "";
  }

  public static String getSuffixBy(String input, int ch) {
    int i = input.lastIndexOf(ch);
    if (i == -1) {
      return input;
    }
    return input.substring(i);
  }

  public static String removeSuffixBy(String input, int ch) {
    int i = input.lastIndexOf(ch);
    if (i == -1) {
      return input;
    }
    return input.substring(0, i);
  }

  public static String stripQuotesFromStringValue(String input) {
    if (input != null && input.length() >= 2
        && input.charAt(0) == '\"' && input.charAt(input.length() - 1) == '\"') {
      return input.substring(1, input.length() - 1);
    }
    return input;
  }
}
