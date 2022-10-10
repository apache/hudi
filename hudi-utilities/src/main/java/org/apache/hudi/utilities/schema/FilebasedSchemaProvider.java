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

package org.apache.hudi.utilities.schema;

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.FileIOUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieIOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter.SourceFormatAdapterConfig.AVRO_FIELD_NAME_INVALID_CHAR_MASK;
import static org.apache.hudi.utilities.deltastreamer.SourceFormatAdapter.SourceFormatAdapterConfig.SANITIZE_AVRO_FIELD_NAMES;

/**
 * A simple schema provider, that reads off files on DFS.
 */
public class FilebasedSchemaProvider extends SchemaProvider {

  private static final String AVRO_FIELD_NAME_KEY = "name";

  /**
   * Configs supported.
   */
  public static class Config {
    private static final String SOURCE_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.source.schema.file";
    private static final String TARGET_SCHEMA_FILE_PROP = "hoodie.deltastreamer.schemaprovider.target.schema.file";
  }

  private final FileSystem fs;

  protected Schema sourceSchema;

  protected Schema targetSchema;

  private static List<Object> transformList(List<Object> src, String invalidCharMask) {
    return src.stream().map(obj -> {
      if (obj instanceof List) {
        return transformList((List<Object>) obj, invalidCharMask);
      } else if (obj instanceof Map) {
        return transformMap((Map<String, Object>) obj, invalidCharMask);
      } else {
        return obj;
      }
    }).collect(Collectors.toList());
  }

  private static Map<String, Object> transformMap(Map<String, Object> src, String invalidCharMask) {
    return src.entrySet().stream()
        .map(kv -> {
          if (kv.getValue() instanceof List) {
            return Pair.of(kv.getKey(), transformList((List<Object>) kv.getValue(), invalidCharMask));
          } else if (kv.getValue() instanceof Map) {
            return Pair.of(kv.getKey(), transformMap((Map<String, Object>) kv.getValue(), invalidCharMask));
          } else if (kv.getValue() instanceof String) {
            String currentStrValue = (String) kv.getValue();
            if (kv.getKey().equals(AVRO_FIELD_NAME_KEY)) {
              return Pair.of(kv.getKey(), HoodieAvroUtils.sanitizeName(currentStrValue, invalidCharMask));
            }
            return Pair.of(kv.getKey(), currentStrValue);
          } else {
            return Pair.of(kv.getKey(), kv.getValue());
          }
        }).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  private static Option<Schema> parseSanitizedAvroSchemaNoThrow(String schemaStr, String invalidCharMask) {
    try {
      ObjectMapper objectMapper = new ObjectMapper();
      objectMapper.enable(JsonParser.Feature.ALLOW_COMMENTS);
      Map<String, Object> objMap = objectMapper.readValue(schemaStr, Map.class);
      Map<String, Object> modifiedMap = transformMap(objMap, invalidCharMask);
      return Option.of(new Schema.Parser().parse(objectMapper.writeValueAsString(modifiedMap)));
    } catch (Exception ex) {
      return Option.empty();
    }
  }

  /*
   * We first rely on Avro to parse and then try to rename only for those failed.
   * This way we can improve our parsing capabilities without breaking existing functionality.
   * For example we don't yet support multiple named schemas defined in a file.
   */
  private static Schema parseAvroSchema(String schemaStr, boolean sanitizeSchema, String invalidCharMask) {
    try {
      return new Schema.Parser().parse(schemaStr);
    } catch (SchemaParseException spe) {
      // if sanitizing is not enabled rethrow the exception.
      if (!sanitizeSchema) {
        throw spe;
      }
      // Rename avro fields and try parsing once again.
      Option<Schema> parseResult = parseSanitizedAvroSchemaNoThrow(schemaStr, invalidCharMask);
      if (!parseResult.isPresent()) {
        // throw original exception.
        throw spe;
      }
      return parseResult.get();
    }
  }

  private static Schema readAvroSchemaFromFile(String schemaPath, FileSystem fs, boolean sanitizeSchema, String invalidCharMask) {
    String schemaStr;
    FSDataInputStream in = null;
    try {
      in = fs.open(new Path(schemaPath));
      schemaStr = FileIOUtils.readAsUTFString(in);
    } catch (IOException ioe) {
      throw new HoodieIOException(String.format("Error reading schema from file %s", schemaPath), ioe);
    } finally {
      if (in != null) {
        IOUtils.closeStream(in);
      }
    }
    return parseAvroSchema(schemaStr, sanitizeSchema, invalidCharMask);
  }

  public FilebasedSchemaProvider(TypedProperties props, JavaSparkContext jssc) {
    super(props, jssc);
    DataSourceUtils.checkRequiredProperties(props, Collections.singletonList(Config.SOURCE_SCHEMA_FILE_PROP));
    String sourceFile = props.getString(Config.SOURCE_SCHEMA_FILE_PROP);
    boolean sanitizeSchema = props.getBoolean(SANITIZE_AVRO_FIELD_NAMES.key(), SANITIZE_AVRO_FIELD_NAMES.defaultValue());
    String invalidCharMask = props.getString(AVRO_FIELD_NAME_INVALID_CHAR_MASK.key(), AVRO_FIELD_NAME_INVALID_CHAR_MASK.defaultValue());
    this.fs = FSUtils.getFs(sourceFile, jssc.hadoopConfiguration(), true);
    this.sourceSchema = readAvroSchemaFromFile(sourceFile, this.fs, sanitizeSchema, invalidCharMask);
    if (props.containsKey(Config.TARGET_SCHEMA_FILE_PROP)) {
      this.targetSchema = readAvroSchemaFromFile(props.getString(Config.TARGET_SCHEMA_FILE_PROP), this.fs, sanitizeSchema, invalidCharMask);
    }
  }

  @Override
  public Schema getSourceSchema() {
    return sourceSchema;
  }

  @Override
  public Schema getTargetSchema() {
    if (targetSchema != null) {
      return targetSchema;
    } else {
      return super.getTargetSchema();
    }
  }
}
