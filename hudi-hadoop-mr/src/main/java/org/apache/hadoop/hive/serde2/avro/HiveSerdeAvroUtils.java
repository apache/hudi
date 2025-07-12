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

package org.apache.hadoop.hive.serde2.avro;

import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities useful only to the AvroSerde itself.  Not mean to be used by
 * end-users but public for interop to the ql package.
 * Taken from https://github.com/apache/hive/blob/01cc7ca672320447b20bc9883e2598d9fb34fc10/serde/src/java/org/apache/hadoop/hive/serde2/avro/AvroSerdeUtils.java
 */
public class HiveSerdeAvroUtils {
  private static final Logger LOG = LoggerFactory.getLogger(HiveSerdeAvroUtils.class);

  /**
   * Enum container for all avro table properties.
   * If introducing a new avro-specific table property,
   * add it here. Putting them in an enum rather than separate strings
   * allows them to be programmatically grouped and referenced together.
   */
  public static enum AvroTableProperties {
    SCHEMA_LITERAL("avro.schema.literal"),
    SCHEMA_URL("avro.schema.url"),
    SCHEMA_NAMESPACE("avro.schema.namespace"),
    SCHEMA_NAME("avro.schema.name"),
    SCHEMA_DOC("avro.schema.doc"),
    AVRO_SERDE_SCHEMA("avro.serde.schema"),
    AVRO_SERDE_TYPE("avro.serde.type"),
    AVRO_SERDE_SKIP_BYTES("avro.serde.skip.bytes"),
    SCHEMA_RETRIEVER("avro.schema.retriever");

    private final String propName;

    AvroTableProperties(String propName) {
      this.propName = propName;
    }

    public String getPropName() {
      return this.propName;
    }
  }

  // Following parameters slated for removal, prefer usage of enum above, that allows programmatic access.
  @Deprecated public static final String SCHEMA_LITERAL = "avro.schema.literal";
  @Deprecated public static final String SCHEMA_URL = "avro.schema.url";
  @Deprecated public static final String SCHEMA_NAMESPACE = "avro.schema.namespace";
  @Deprecated public static final String SCHEMA_NAME = "avro.schema.name";
  @Deprecated public static final String SCHEMA_DOC = "avro.schema.doc";
  @Deprecated public static final String AVRO_SERDE_SCHEMA = AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName();
  @Deprecated public static final String SCHEMA_RETRIEVER = AvroTableProperties.SCHEMA_RETRIEVER.getPropName();

  public static final String SCHEMA_NONE = "none";
  public static final String EXCEPTION_MESSAGE = "Neither "
      + AvroTableProperties.SCHEMA_LITERAL.getPropName() + " nor "
      + AvroTableProperties.SCHEMA_URL.getPropName() + " specified, can't determine table schema";

  /**
   * Determine the schema to that's been provided for Avro serde work.
   * @param properties containing a key pointing to the schema, one way or another
   * @return schema to use while serdeing the avro file
   * @throws IOException if error while trying to read the schema from another location
   * @throws AvroSerdeException if unable to find a schema or pointer to it in the properties
   */
  public static Schema determineSchemaOrThrowException(Configuration conf, Properties properties)
      throws IOException, AvroSerdeException {
    String schemaString = properties.getProperty(AvroTableProperties.SCHEMA_LITERAL.getPropName());
    if (schemaString != null && !schemaString.equals(SCHEMA_NONE)) {
      return AvroSerdeUtils.getSchemaFor(schemaString);
    }

    // Try pulling directly from URL
    schemaString = properties.getProperty(AvroTableProperties.SCHEMA_URL.getPropName());
    if (schemaString == null) {
      final String columnNameProperty = properties.getProperty(serdeConstants.LIST_COLUMNS);
      final String columnTypeProperty = properties.getProperty(serdeConstants.LIST_COLUMN_TYPES);
      final String columnCommentProperty = properties.getProperty(AvroSerDe.LIST_COLUMN_COMMENTS);
      if (columnNameProperty == null || columnNameProperty.isEmpty()
          || columnTypeProperty == null || columnTypeProperty.isEmpty()) {
        throw new AvroSerdeException(EXCEPTION_MESSAGE);
      }
      final String columnNameDelimiter = properties.containsKey(serdeConstants.COLUMN_NAME_DELIMITER) ? properties
          .getProperty(serdeConstants.COLUMN_NAME_DELIMITER) : String.valueOf(SerDeUtils.COMMA);
      // Get column names and types
      List<String> columnNames = Arrays.asList(columnNameProperty.split(columnNameDelimiter));
      List<TypeInfo> columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

      Schema schema = AvroSerDe.getSchemaFromCols(properties, columnNames, columnTypes, columnCommentProperty);
      properties.setProperty(AvroTableProperties.SCHEMA_LITERAL.getPropName(), schema.toString());
      if (conf != null) {
        conf.set(AvroTableProperties.AVRO_SERDE_SCHEMA.getPropName(), schema.toString(false));
      }
      return schema;
    } else if (schemaString.equals(SCHEMA_NONE)) {
      throw new AvroSerdeException(EXCEPTION_MESSAGE);
    }

    try {
      Schema s = getSchemaFromFS(schemaString, conf);
      if (s == null) {
        //in case schema is not a file system
        return AvroSerdeUtils.getSchemaFor(new URL(schemaString));
      }
      return s;
    } catch (IOException ioe) {
      throw new AvroSerdeException("Unable to read schema from given path: " + schemaString, ioe);
    } catch (URISyntaxException urie) {
      throw new AvroSerdeException("Unable to read schema from given path: " + schemaString, urie);
    }
  }

  // Protected for testing and so we can pass in a conf for testing.
  protected static Schema getSchemaFromFS(String schemaFSUrl,
                                          Configuration conf) throws IOException, URISyntaxException {
    FSDataInputStream in = null;
    FileSystem fs = null;
    try {
      fs = FileSystem.get(new URI(schemaFSUrl), conf);
    } catch (IOException ioe) {
      //return null only if the file system in schema is not recognized
      LOG.debug("Failed to open file system for uri {} assuming it is not a FileSystem url", schemaFSUrl, ioe);
      return null;
    }
    try {
      in = fs.open(new Path(schemaFSUrl));
      Schema s = AvroSerdeUtils.getSchemaFor(in);
      return s;
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  /**
   * Determine if an Avro schema is of type Union[T, NULL].  Avro supports nullable
   * types via a union of type T and null.  This is a very common use case.
   * As such, we want to silently convert it to just T and allow the value to be null.
   *
   * When a Hive union type is used with AVRO, the schema type becomes
   * Union[NULL, T1, T2, ...]. The NULL in the union should be silently removed
   *
   * @return true if type represents Union[T, Null], false otherwise
   */
  public static boolean isNullableType(Schema schema) {
    if (!schema.getType().equals(Schema.Type.UNION)) {
      return false;
    }

    List<Schema> itemSchemas = schema.getTypes();
    if (itemSchemas.size() < 2) {
      return false;
    }

    for (Schema itemSchema : itemSchemas) {
      if (Schema.Type.NULL.equals(itemSchema.getType())) {
        return true;
      }
    }

    // [null, null] not allowed, so this check is ok.
    return false;
  }

  /**
   * If the union schema is a nullable union, get the schema for the non-nullable type.
   * This method does no checking that the provided Schema is nullable. If the provided
   * union schema is non-nullable, it simply returns the union schema
   */
  public static Schema getOtherTypeFromNullableType(Schema unionSchema) {
    final List<Schema> types = unionSchema.getTypes();
    if (types.size() == 2) { // most common scenario
      if (types.get(0).getType() == Schema.Type.NULL) {
        return types.get(1);
      }
      if (types.get(1).getType() == Schema.Type.NULL) {
        return types.get(0);
      }
      // not a nullable union
      return unionSchema;
    }

    final List<Schema> itemSchemas = new ArrayList<>();
    for (Schema itemSchema : types) {
      if (!Schema.Type.NULL.equals(itemSchema.getType())) {
        itemSchemas.add(itemSchema);
      }
    }

    if (itemSchemas.size() > 1) {
      return Schema.createUnion(itemSchemas);
    } else {
      return itemSchemas.get(0);
    }
  }

  /**
   * Determine if we're being executed from within an MR job or as part
   * of a select * statement.  The signals for this varies between Hive versions.
   * @param job that contains things that are or are not set in a job
   * @return Are we in a job or not?
   */
  public static boolean insideMRJob(JobConf job) {
    return job != null
        && (HiveConf.getVar(job, HiveConf.ConfVars.PLAN) != null)
        && (!HiveConf.getVar(job, HiveConf.ConfVars.PLAN).isEmpty());
  }

  public static Buffer getBufferFromBytes(byte[] input) {
    ByteBuffer bb = ByteBuffer.wrap(input);
    return bb.rewind();
  }

  public static Buffer getBufferFromDecimal(HiveDecimal dec, int scale) {
    if (dec == null) {
      return null;
    }

    // NOTE: Previously, we did OldHiveDecimal.setScale(scale), called OldHiveDecimal
    //       unscaledValue().toByteArray().
    return AvroSerdeUtils.getBufferFromBytes(dec.bigIntegerBytesScaled(scale));
  }

  public static byte[] getBytesFromByteBuffer(ByteBuffer byteBuffer) {
    byteBuffer.rewind();
    byte[] result = new byte[byteBuffer.limit()];
    byteBuffer.get(result);
    return result;
  }

  public static HiveDecimal getHiveDecimalFromByteBuffer(ByteBuffer byteBuffer, int scale) {
    byte[] result = getBytesFromByteBuffer(byteBuffer);
    HiveDecimal dec = HiveDecimal.create(new BigInteger(result), scale);
    return dec;
  }

  private static Schema.Parser getSchemaParser() {
    // HIVE-24797: Disable validate default values when parsing Avro schemas.
    return new Schema.Parser().setValidateDefaults(false);
  }

  public static Schema getSchemaFor(String str) {
    Schema schema = getSchemaParser().parse(str);
    return schema;
  }

  public static Schema getSchemaFor(File file) {
    Schema schema;
    try {
      schema = getSchemaParser().parse(file);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse Avro schema from " + file.getName(), e);
    }
    return schema;
  }

  public static Schema getSchemaFor(InputStream stream) {
    Schema schema;
    try {
      schema = getSchemaParser().parse(stream);
    } catch (IOException e) {
      throw new RuntimeException("Failed to parse Avro schema", e);
    }
    return schema;
  }

  public static Schema getSchemaFor(URL url) {
    InputStream in = null;
    try {
      in = url.openStream();
      return getSchemaFor(in);
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse Avro schema", e);
    } finally {
      if (in != null) {
        try {
          in.close();
        } catch (IOException e) {
          // Ignore
        }
      }
    }
  }

  public static int getIntFromSchema(Schema schema, String name) {
    Object obj = schema.getObjectProp(name);
    if (obj instanceof String) {
      return Integer.parseInt((String) obj);
    } else if (obj instanceof Integer) {
      return (int) obj;
    } else {
      throw new IllegalArgumentException("Expect integer or string value from property " + name
          + " but found type " + obj.getClass().getName());
    }
  }

  /**
   * Called on specific alter table events, removes schema url and schema literal from given tblproperties
   * After the change, HMS solely will be responsible for handling the schema
   *
   * @param conf
   * @param serializationLib
   * @param parameters
   */
  public static void handleAlterTableForAvro(HiveConf conf, String serializationLib, Map<String, String> parameters) {
    if (AvroSerDe.class.getName().equals(serializationLib)) {
      String literalPropName = AvroTableProperties.SCHEMA_LITERAL.getPropName();
      String urlPropName = AvroTableProperties.SCHEMA_URL.getPropName();

      if (parameters.containsKey(literalPropName) || parameters.containsKey(urlPropName)) {
        throw new RuntimeException("Not allowed to alter schema of Avro stored table having external schema."
            + " Consider removing " + AvroTableProperties.SCHEMA_LITERAL.getPropName() + " or "
            + AvroTableProperties.SCHEMA_URL.getPropName() + " from table properties.");
      }
    }
  }
}