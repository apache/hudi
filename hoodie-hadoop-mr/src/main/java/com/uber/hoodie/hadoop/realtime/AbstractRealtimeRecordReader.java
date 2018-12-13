/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.hadoop.realtime;

import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.log.HoodieLogFormat;
import com.uber.hoodie.common.table.log.HoodieLogFormat.Reader;
import com.uber.hoodie.common.table.log.block.HoodieAvroDataBlock;
import com.uber.hoodie.common.table.log.block.HoodieLogBlock;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.exception.HoodieException;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericFixed;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import parquet.avro.AvroSchemaConverter;
import parquet.hadoop.ParquetFileReader;
import parquet.schema.MessageType;

/**
 * Record Reader implementation to merge fresh avro data with base parquet data, to support real
 * time queries.
 */
public abstract class AbstractRealtimeRecordReader {

  // Fraction of mapper/reducer task memory used for compaction of log files
  public static final String COMPACTION_MEMORY_FRACTION_PROP = "compaction.memory.fraction";
  public static final String DEFAULT_COMPACTION_MEMORY_FRACTION = "0.75";
  // used to choose a trade off between IO vs Memory when performing compaction process
  // Depending on outputfile size and memory provided, choose true to avoid OOM for large file
  // size + small memory
  public static final String COMPACTION_LAZY_BLOCK_READ_ENABLED_PROP =
      "compaction.lazy.block.read.enabled";
  public static final String DEFAULT_COMPACTION_LAZY_BLOCK_READ_ENABLED = "true";

  // Property to set the max memory for dfs inputstream buffer size
  public static final String MAX_DFS_STREAM_BUFFER_SIZE_PROP = "hoodie.memory.dfs.buffer.max.size";
  // Setting this to lower value of 1 MB since no control over how many RecordReaders will be started in a mapper
  public static final int DEFAULT_MAX_DFS_STREAM_BUFFER_SIZE = 1 * 1024 * 1024; // 1 MB
  // Property to set file path prefix for spillable file
  public static final String SPILLABLE_MAP_BASE_PATH_PROP = "hoodie.memory.spillable.map.path";
  // Default file path prefix for spillable file
  public static final String DEFAULT_SPILLABLE_MAP_BASE_PATH = "/tmp/";

  public static final Log LOG = LogFactory.getLog(AbstractRealtimeRecordReader.class);
  protected final HoodieRealtimeFileSplit split;
  protected final JobConf jobConf;
  private final MessageType baseFileSchema;

  // Schema handles
  private Schema readerSchema;
  private Schema writerSchema;

  public AbstractRealtimeRecordReader(HoodieRealtimeFileSplit split, JobConf job) {
    this.split = split;
    this.jobConf = job;

    LOG.info("cfg ==> " + job.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR));
    try {
      baseFileSchema = readSchema(jobConf, split.getPath());
      init();
    } catch (IOException e) {
      throw new HoodieIOException(
          "Could not create HoodieRealtimeRecordReader on path " + this.split.getPath(), e);
    }
  }

  /**
   * Reads the schema from the parquet file. This is different from ParquetUtils as it uses the
   * twitter parquet to support hive 1.1.0
   */
  private static MessageType readSchema(Configuration conf, Path parquetFilePath) {
    try {
      return ParquetFileReader.readFooter(conf, parquetFilePath).getFileMetaData().getSchema();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to read footer for parquet " + parquetFilePath, e);
    }
  }

  protected static String arrayWritableToString(ArrayWritable writable) {
    if (writable == null) {
      return "null";
    }

    StringBuilder builder = new StringBuilder();
    Writable[] values = writable.get();
    builder.append(String.format("(Size: %s)[", values.length));
    for (Writable w : values) {
      if (w instanceof ArrayWritable) {
        builder.append(arrayWritableToString((ArrayWritable) w)).append(" ");
      } else {
        builder.append(w).append(" ");
      }
    }
    builder.append("]");
    return builder.toString();
  }

  /**
   * Given a comma separated list of field names and positions at which they appear on Hive, return
   * a ordered list of field names, that can be passed onto storage.
   */
  private static List<String> orderFields(String fieldNameCsv, String fieldOrderCsv, List<String> partitioningFields) {

    String[] fieldOrders = fieldOrderCsv.split(",");
    List<String> fieldNames = Arrays.stream(fieldNameCsv.split(","))
        .filter(fn -> !partitioningFields.contains(fn)).collect(Collectors.toList());

    // Hive does not provide ids for partitioning fields, so check for lengths excluding that.
    if (fieldNames.size() != fieldOrders.length) {
      throw new HoodieException(String
          .format("Error ordering fields for storage read. #fieldNames: %d, #fieldPositions: %d",
              fieldNames.size(), fieldOrders.length));
    }
    TreeMap<Integer, String> orderedFieldMap = new TreeMap<>();
    for (int ox = 0; ox < fieldOrders.length; ox++) {
      orderedFieldMap.put(Integer.parseInt(fieldOrders[ox]), fieldNames.get(ox));
    }
    return new ArrayList<>(orderedFieldMap.values());
  }

  /**
   * Generate a reader schema off the provided writeSchema, to just project out the provided
   * columns
   */
  public static Schema generateProjectionSchema(Schema writeSchema, List<String> fieldNames) {
    /**
     * Avro & Presto field names seems to be case sensitive (support fields differing only in case)
     * whereas Hive/Impala/SparkSQL(default) are case-insensitive. Spark allows this to be configurable
     * using spark.sql.caseSensitive=true
     *
     * For a RT table setup with no delta-files (for a latest file-slice) -> we translate parquet schema to Avro
     * Here the field-name case is dependent on parquet schema. Hive (1.x/2.x/CDH) translate column projections
     * to lower-cases
     *
     */
    List<Schema.Field> projectedFields = new ArrayList<>();
    Map<String, Schema.Field> schemaFieldsMap = writeSchema.getFields().stream()
        .map(r -> Pair.of(r.name().toLowerCase(), r)).collect(Collectors.toMap(Pair::getLeft, Pair::getRight));
    for (String fn : fieldNames) {
      Schema.Field field = schemaFieldsMap.get(fn.toLowerCase());
      if (field == null) {
        throw new HoodieException("Field " + fn + " not found in log schema. Query cannot proceed! "
            + "Derived Schema Fields: "
            + new ArrayList<>(schemaFieldsMap.keySet()));
      }
      projectedFields
          .add(new Schema.Field(field.name(), field.schema(), field.doc(), field.defaultValue()));
    }

    Schema projectedSchema = Schema
        .createRecord(writeSchema.getName(), writeSchema.getDoc(), writeSchema.getNamespace(), writeSchema.isError());
    projectedSchema.setFields(projectedFields);
    return projectedSchema;
  }

  /**
   * Convert the projected read from delta record into an array writable
   */
  public static Writable avroToArrayWritable(Object value, Schema schema) {

    // if value is null, make a NullWritable
    // Hive 2.x does not like NullWritable
    if (value == null) {

      return null;
      //return NullWritable.get();
    }


    Writable[] wrapperWritable;

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
        // return NullWritable.get();
      case RECORD:
        GenericRecord record = (GenericRecord) value;
        Writable[] values1 = new Writable[schema.getFields().size()];
        int index1 = 0;
        for (Schema.Field field : schema.getFields()) {
          values1[index1++] = avroToArrayWritable(record.get(field.name()), field.schema());
        }
        return new ArrayWritable(Writable.class, values1);
      case ENUM:
        return new Text(value.toString());
      case ARRAY:
        GenericArray arrayValue = (GenericArray) value;
        Writable[] values2 = new Writable[arrayValue.size()];
        int index2 = 0;
        for (Object obj : arrayValue) {
          values2[index2++] = avroToArrayWritable(obj, schema.getElementType());
        }
        wrapperWritable = new Writable[]{new ArrayWritable(Writable.class, values2)};
        return new ArrayWritable(Writable.class, wrapperWritable);
      case MAP:
        Map mapValue = (Map) value;
        Writable[] values3 = new Writable[mapValue.size()];
        int index3 = 0;
        for (Object entry : mapValue.entrySet()) {
          Map.Entry mapEntry = (Map.Entry) entry;
          Writable[] mapValues = new Writable[2];
          mapValues[0] = new Text(mapEntry.getKey().toString());
          mapValues[1] = avroToArrayWritable(mapEntry.getValue(), schema.getValueType());
          values3[index3++] = new ArrayWritable(Writable.class, mapValues);
        }
        wrapperWritable = new Writable[]{new ArrayWritable(Writable.class, values3)};
        return new ArrayWritable(Writable.class, wrapperWritable);
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

  public static Schema readSchemaFromLogFile(FileSystem fs, Path path) throws IOException {
    Reader reader = HoodieLogFormat.newReader(fs, new HoodieLogFile(path), null);
    HoodieAvroDataBlock lastBlock = null;
    while (reader.hasNext()) {
      HoodieLogBlock block = reader.next();
      if (block instanceof HoodieAvroDataBlock) {
        lastBlock = (HoodieAvroDataBlock) block;
      }
    }
    reader.close();
    if (lastBlock != null) {
      return lastBlock.getSchema();
    }
    return null;
  }

  /**
   * Hive implementation of ParquetRecordReader results in partition columns not present in the original parquet file
   * to also be part of the projected schema. Hive expects the record reader implementation to return the row in its
   * entirety (with un-projected column having null values). As we use writerSchema for this, make sure writer schema
   * also includes partition columns
   * @param schema Schema to be changed
   * @return
   */
  private static Schema addPartitionFields(Schema schema, List<String> partitioningFields) {
    final Set<String> firstLevelFieldNames = schema.getFields().stream().map(Field::name)
        .map(String::toLowerCase).collect(Collectors.toSet());
    List<String> fieldsToAdd = partitioningFields.stream().map(String::toLowerCase)
        .filter(x -> !firstLevelFieldNames.contains(x)).collect(Collectors.toList());

    return HoodieAvroUtils.appendNullSchemaFields(schema, fieldsToAdd);
  }

  /**
   * Goes through the log files and populates a map with latest version of each key logged, since
   * the base split was written.
   */
  private void init() throws IOException {
    writerSchema = new AvroSchemaConverter().convert(baseFileSchema);
    List<String> fieldNames = writerSchema.getFields().stream().map(Field::name).collect(Collectors.toList());
    if (split.getDeltaFilePaths().size() > 0) {
      String logPath = split.getDeltaFilePaths().get(split.getDeltaFilePaths().size() - 1);
      FileSystem fs = FSUtils.getFs(logPath, jobConf);
      writerSchema = readSchemaFromLogFile(fs, new Path(logPath));
      fieldNames = writerSchema.getFields().stream().map(Field::name).collect(Collectors.toList());
    }

    // Add partitioning fields to writer schema for resulting row to contain null values for these fields
    List<String> partitioningFields = Arrays.stream(
        jobConf.get("partition_columns", "").split(",")).collect(Collectors.toList());
    writerSchema = addPartitionFields(writerSchema, partitioningFields);

    List<String> projectionFields = orderFields(
        jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
        jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR),
        partitioningFields);
    // TODO(vc): In the future, the reader schema should be updated based on log files & be able
    // to null out fields not present before
    readerSchema = generateProjectionSchema(writerSchema, projectionFields);

    LOG.info(String.format("About to read compacted logs %s for base split %s, projecting cols %s",
        split.getDeltaFilePaths(), split.getPath(), projectionFields));
  }

  public Schema getReaderSchema() {
    return readerSchema;
  }

  public Schema getWriterSchema() {
    return writerSchema;
  }

  public long getMaxCompactionMemoryInBytes() {
    return (long) Math.ceil(Double
        .valueOf(jobConf.get(COMPACTION_MEMORY_FRACTION_PROP, DEFAULT_COMPACTION_MEMORY_FRACTION))
        * jobConf.getMemoryForMapTask());
  }
}
