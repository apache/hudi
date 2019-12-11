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

package org.apache.hudi.hadoop.realtime;

import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieAvroUtils;
import org.apache.hudi.common.util.LogReaderUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.schema.MessageType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Record Reader implementation to merge fresh avro data with base parquet data, to support real time queries.
 */
public abstract class AbstractRealtimeRecordReader {

  // Fraction of mapper/reducer task memory used for compaction of log files
  public static final String COMPACTION_MEMORY_FRACTION_PROP = "compaction.memory.fraction";
  public static final String DEFAULT_COMPACTION_MEMORY_FRACTION = "0.75";
  // used to choose a trade off between IO vs Memory when performing compaction process
  // Depending on outputfile size and memory provided, choose true to avoid OOM for large file
  // size + small memory
  public static final String COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP = "compaction.lazy.block.read.enabled";
  public static final String DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED = "true";

  // Property to set the max memory for dfs inputstream buffer size
  public static final String MAX_DFS_STREAM_BUFFER_SIZE_PROP = "hoodie.memory.dfs.buffer.max.size";
  // Setting this to lower value of 1 MB since no control over how many RecordReaders will be started in a mapper
  public static final int DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE = 1 * 1024 * 1024; // 1 MB
  // Property to set file path prefix for spillable file
  public static final String SPILLABLE_MAP_BASE_PATH_PROP = "hoodie.memory.spillable.map.path";
  // Default file path prefix for spillable file
  public static final String DEFAULT_SPILLABLE_MAP_BASE_PATH = "/tmp/";

  private static final Logger LOG = LogManager.getLogger(AbstractRealtimeRecordReader.class);

  protected final HoodieRealtimeFileSplit split;
  protected final JobConf jobConf;
  private final MessageType baseFileSchema;
  protected final boolean usesCustomPayload;
  // Schema handles
  private Schema readerSchema;
  private Schema writerSchema;
  private Schema hiveSchema;

  public AbstractRealtimeRecordReader(HoodieRealtimeFileSplit split, JobConf job) {
    this.split = split;
    this.jobConf = job;
    LOG.info("cfg ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
    LOG.info("columnIds ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR));
    LOG.info("partitioningColumns ==> " + job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, ""));
    try {
      this.usesCustomPayload = usesCustomPayload();
      LOG.info("usesCustomPayload ==> " + this.usesCustomPayload);
      baseFileSchema = readSchema(jobConf, split.getPath());
      init();
    } catch (IOException e) {
      throw new HoodieIOException("Could not create HoodieRealtimeRecordReader on path " + this.split.getPath(), e);
    }
  }

  private boolean usesCustomPayload() {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jobConf, split.getBasePath());
    return !(metaClient.getTableConfig().getPayloadClass().contains(HoodieAvroPayload.class.getName())
        || metaClient.getTableConfig().getPayloadClass().contains("org.apache.hudi.OverwriteWithLatestAvroPayload"));
  }

  /**
   * Reads the schema from the parquet file. This is different from ParquetUtils as it uses the twitter parquet to
   * support hive 1.1.0
   */
  private static MessageType readSchema(Configuration conf, Path parquetFilePath) {
    try {
      return ParquetFileReader.readFooter(conf, parquetFilePath).getFileMetaData().getSchema();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read footer for parquet " + parquetFilePath, e);
    }
  }

  /**
   * Prints a JSON representation of the ArrayWritable for easier debuggability.
   */
  protected static String arrayWritableToString(ArrayWritable writable) {
    if (writable == null) {
      return "null";
    }
    StringBuilder builder = new StringBuilder();
    Writable[] values = writable.get();
    builder.append("\"values_" + Math.random() + "_" + values.length + "\": {");
    int i = 0;
    for (Writable w : values) {
      if (w instanceof ArrayWritable) {
        builder.append(arrayWritableToString((ArrayWritable) w)).append(",");
      } else {
        builder.append("\"value" + i + "\":" + "\"" + w + "\"").append(",");
        if (w == null) {
          builder.append("\"type" + i + "\":" + "\"unknown\"").append(",");
        } else {
          builder.append("\"type" + i + "\":" + "\"" + w.getClass().getSimpleName() + "\"").append(",");
        }
      }
      i++;
    }
    builder.deleteCharAt(builder.length() - 1);
    builder.append("}");
    return builder.toString();
  }

  /**
   * Given a comma separated list of field names and positions at which they appear on Hive, return
   * an ordered list of field names, that can be passed onto storage.
   */
  private static List<String> orderFields(String fieldNameCsv, String fieldOrderCsv, List<String> partitioningFields) {
    // Need to convert the following to Set first since Hive does not handle duplicate field names correctly but
    // handles duplicate fields orders correctly.
    // Fields Orders -> {@link https://github
    // .com/apache/hive/blob/f37c5de6c32b9395d1b34fa3c02ed06d1bfbf6eb/serde/src/java
    // /org/apache/hadoop/hive/serde2/ColumnProjectionUtils.java#L188}
    // Field Names -> {@link https://github.com/apache/hive/blob/f37c5de6c32b9395d1b34fa3c02ed06d1bfbf6eb/serde/src/java
    // /org/apache/hadoop/hive/serde2/ColumnProjectionUtils.java#L229}
    Set<String> fieldOrdersSet = new LinkedHashSet<>();
    String[] fieldOrdersWithDups = fieldOrderCsv.split(",");
    for (String fieldOrder : fieldOrdersWithDups) {
      fieldOrdersSet.add(fieldOrder);
    }
    String[] fieldOrders = fieldOrdersSet.toArray(new String[fieldOrdersSet.size()]);
    List<String> fieldNames = Arrays.stream(fieldNameCsv.split(","))
        .filter(fn -> !partitioningFields.contains(fn)).collect(Collectors.toList());
    Set<String> fieldNamesSet = new LinkedHashSet<>();
    for (String fieldName : fieldNames) {
      fieldNamesSet.add(fieldName);
    }
    // Hive does not provide ids for partitioning fields, so check for lengths excluding that.
    if (fieldNamesSet.size() != fieldOrders.length) {
      throw new HoodieException(String
          .format("Error ordering fields for storage read. #fieldNames: %d, #fieldPositions: %d",
              fieldNames.size(), fieldOrders.length));
    }
    TreeMap<Integer, String> orderedFieldMap = new TreeMap<>();
    String[] fieldNamesArray = fieldNamesSet.toArray(new String[fieldNamesSet.size()]);
    for (int ox = 0; ox < fieldOrders.length; ox++) {
      orderedFieldMap.put(Integer.parseInt(fieldOrders[ox]), fieldNamesArray[ox]);
    }
    return new ArrayList<>(orderedFieldMap.values());
  }

  /**
   * Generate a reader schema off the provided writeSchema, to just project out the provided columns.
   */
  public static Schema generateProjectionSchema(Schema writeSchema, Map<String, Field> schemaFieldsMap,
      List<String> fieldNames) {
    /**
     * Avro & Presto field names seems to be case sensitive (support fields differing only in case) whereas
     * Hive/Impala/SparkSQL(default) are case-insensitive. Spark allows this to be configurable using
     * spark.sql.caseSensitive=true
     *
     * For a RT table setup with no delta-files (for a latest file-slice) -> we translate parquet schema to Avro Here
     * the field-name case is dependent on parquet schema. Hive (1.x/2.x/CDH) translate column projections to
     * lower-cases
     *
     */
    List<Schema.Field> projectedFields = new ArrayList<>();
    for (String fn : fieldNames) {
      Schema.Field field = schemaFieldsMap.get(fn.toLowerCase());
      if (field == null) {
        throw new HoodieException("Field " + fn + " not found in log schema. Query cannot proceed! "
            + "Derived Schema Fields: " + new ArrayList<>(schemaFieldsMap.keySet()));
      } else {
        projectedFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
      }
    }

    Schema projectedSchema = Schema.createRecord(writeSchema.getName(), writeSchema.getDoc(),
        writeSchema.getNamespace(), writeSchema.isError());
    projectedSchema.setFields(projectedFields);
    return projectedSchema;
  }

  public static Map<String, Field> getNameToFieldMap(Schema schema) {
    return schema.getFields().stream().map(r -> Pair.of(r.name().toLowerCase(), r))
        .collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
  }

  /**
   * Convert the projected read from delta record into an array writable.
   */
  public static Writable avroToArrayWritable(Object value, Schema schema) {

    if (value == null) {
      return null;
    }

    switch (schema.getType()) {
      case STRING:
        return new Text(value.toString());
      case BYTES:
        return new BytesWritable((byte[]) value);
      case INT:
        return new IntWritable((Integer) value);
      case LONG:
        return new LongWritable((Long) value);
      case FLOAT:
        return new FloatWritable((Float) value);
      case DOUBLE:
        return new DoubleWritable((Double) value);
      case BOOLEAN:
        return new BooleanWritable((Boolean) value);
      case NULL:
        return null;
      case RECORD:
        GenericRecord record = (GenericRecord) value;
        Writable[] recordValues = new Writable[schema.getFields().size()];
        int recordValueIndex = 0;
        for (Schema.Field field : schema.getFields()) {
          recordValues[recordValueIndex++] = avroToArrayWritable(record.get(field.name()), field.schema());
        }
        return new ArrayWritable(Writable.class, recordValues);
      case ENUM:
        return new Text(value.toString());
      case ARRAY:
        GenericArray arrayValue = (GenericArray) value;
        Writable[] arrayValues = new Writable[arrayValue.size()];
        int arrayValueIndex = 0;
        for (Object obj : arrayValue) {
          arrayValues[arrayValueIndex++] = avroToArrayWritable(obj, schema.getElementType());
        }
        // Hive 1.x will fail here, it requires values2 to be wrapped into another ArrayWritable
        return new ArrayWritable(Writable.class, arrayValues);
      case MAP:
        Map mapValue = (Map) value;
        Writable[] mapValues = new Writable[mapValue.size()];
        int mapValueIndex = 0;
        for (Object entry : mapValue.entrySet()) {
          Map.Entry mapEntry = (Map.Entry) entry;
          Writable[] nestedMapValues = new Writable[2];
          nestedMapValues[0] = new Text(mapEntry.getKey().toString());
          nestedMapValues[1] = avroToArrayWritable(mapEntry.getValue(), schema.getValueType());
          mapValues[mapValueIndex++] = new ArrayWritable(Writable.class, nestedMapValues);
        }
        // Hive 1.x will fail here, it requires values3 to be wrapped into another ArrayWritable
        return new ArrayWritable(Writable.class, mapValues);
      case UNION:
        List<Schema> types = schema.getTypes();
        if (types.size() != 2) {
          throw new IllegalArgumentException("Only support union with 2 fields");
        }
        Schema s1 = types.get(0);
        Schema s2 = types.get(1);
        if (s1.getType() == Schema.Type.NULL) {
          return avroToArrayWritable(value, s2);
        } else if (s2.getType() == Schema.Type.NULL) {
          return avroToArrayWritable(value, s1);
        } else {
          throw new IllegalArgumentException("Only support union with null");
        }
      case FIXED:
        return new BytesWritable(((GenericFixed) value).bytes());
      default:
        return null;
    }
  }

  /**
   * Hive implementation of ParquetRecordReader results in partition columns not present in the original parquet file to
   * also be part of the projected schema. Hive expects the record reader implementation to return the row in its
   * entirety (with un-projected column having null values). As we use writerSchema for this, make sure writer schema
   * also includes partition columns
   *
   * @param schema Schema to be changed
   */
  private static Schema addPartitionFields(Schema schema, List<String> partitioningFields) {
    final Set<String> firstLevelFieldNames =
        schema.getFields().stream().map(Field::name).map(String::toLowerCase).collect(Collectors.toSet());
    List<String> fieldsToAdd = partitioningFields.stream().map(String::toLowerCase)
        .filter(x -> !firstLevelFieldNames.contains(x)).collect(Collectors.toList());

    return HoodieAvroUtils.appendNullSchemaFields(schema, fieldsToAdd);
  }

  /**
   * Goes through the log files in reverse order and finds the schema from the last available data block. If not, falls
   * back to the schema from the latest parquet file. Finally, sets the partition column and projection fields into the
   * job conf.
   */
  private void init() throws IOException {
    Schema schemaFromLogFile =
        LogReaderUtils.readLatestSchemaFromLogFiles(split.getBasePath(), split.getDeltaFilePaths(), jobConf);
    if (schemaFromLogFile == null) {
      writerSchema = new AvroSchemaConverter().convert(baseFileSchema);
      LOG.debug("Writer Schema From Parquet => " + writerSchema.getFields());
    } else {
      writerSchema = schemaFromLogFile;
      LOG.debug("Writer Schema From Log => " + writerSchema.getFields());
    }
    // Add partitioning fields to writer schema for resulting row to contain null values for these fields
    String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    List<String> partitioningFields =
        partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
            : new ArrayList<>();
    writerSchema = addPartitionFields(writerSchema, partitioningFields);
    List<String> projectionFields = orderFields(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
        jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), partitioningFields);

    Map<String, Field> schemaFieldsMap = getNameToFieldMap(writerSchema);
    hiveSchema = constructHiveOrderedSchema(writerSchema, schemaFieldsMap);
    // TODO(vc): In the future, the reader schema should be updated based on log files & be able
    // to null out fields not present before

    readerSchema = generateProjectionSchema(writerSchema, schemaFieldsMap, projectionFields);
    LOG.info(String.format("About to read compacted logs %s for base split %s, projecting cols %s",
        split.getDeltaFilePaths(), split.getPath(), projectionFields));
  }

  private Schema constructHiveOrderedSchema(Schema writerSchema, Map<String, Field> schemaFieldsMap) {
    // Get all column names of hive table
    String hiveColumnString = jobConf.get(hive_metastoreConstants.META_TABLE_COLUMNS);
    String[] hiveColumns = hiveColumnString.split(",");
    List<Field> hiveSchemaFields = new ArrayList<>();

    for (String columnName : hiveColumns) {
      Field field = schemaFieldsMap.get(columnName.toLowerCase());

      if (field != null) {
        hiveSchemaFields.add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
      } else {
        // Hive has some extra virtual columns like BLOCK__OFFSET__INSIDE__FILE which do not exist in table schema.
        // They will get skipped as they won't be found in the original schema.
        LOG.debug("Skipping Hive Column => " + columnName);
      }
    }

    Schema hiveSchema = Schema.createRecord(writerSchema.getName(), writerSchema.getDoc(), writerSchema.getNamespace(),
        writerSchema.isError());
    hiveSchema.setFields(hiveSchemaFields);
    return hiveSchema;
  }

  public Schema getReaderSchema() {
    return readerSchema;
  }

  public Schema getWriterSchema() {
    return writerSchema;
  }

  public Schema getHiveSchema() {
    return hiveSchema;
  }

  public long getMaxCompactionMemoryInBytes() {
    // jobConf.getMemoryForMapTask() returns in MB
    return (long) Math
        .ceil(Double.valueOf(jobConf.get(COMPACTION_MEMORY_FRACTION_PROP, DEFAULT_COMPACTION_MEMORY_FRACTION))
            * jobConf.getMemoryForMapTask() * 1024 * 1024L);
  }
}
