/*
 * Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.io.storage;

import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.HoodieAvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.EncodingStrategy;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.OrcTableProperties;
import org.apache.hadoop.hive.ql.io.orc.OrcFile.WriterOptions;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.SerDeUtils;
import org.apache.hadoop.hive.serde2.io.DateWritable;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StandardStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.ShortWritable;
import org.apache.hadoop.io.Text;
import org.apache.spark.TaskContext;

/**
 * HoodieOrcWriter use hive's Writer to help limit the size of underlying file. Provides
 * a way to check if the current file can take more records with the <code>canWrite()</code>
 */
public class HoodieOrcWriter<T extends HoodieRecordPayload, R extends IndexedRecord>
    implements HoodieStorageWriter<R> {

  private static AtomicLong recordIndex = new AtomicLong(1);

  private final long maxFileSize;
  private final long orcStripeSize;
  private final Path file;
  private final HoodieWrapperFileSystem fs;
  private final String commitTime;
  private final Writer writer;
  private final List<String> columnNames;
  private final List<TypeInfo> fieldTypes;
  private final List<ObjectInspector> fieldObjectInspectors;
  private final StandardStructObjectInspector hudiObjectInspector;

  public HoodieOrcWriter(String commitTime, Path file, HoodieWriteConfig config,
      Configuration hadoopConfig) throws IOException {
    // default value is 64M
    this.orcStripeSize = config.getOrcStripeSize();
    // default value is 256M
    long orcBlockSize = config.getOrcBlockSize();
    this.maxFileSize = orcBlockSize + (2 * orcStripeSize);

    this.file = HoodieWrapperFileSystem.convertToHoodiePath(file, hadoopConfig);
    this.fs = (HoodieWrapperFileSystem) this.file.getFileSystem(registerFileSystem(file, hadoopConfig));
    this.commitTime = commitTime;

    // Read the configuration parameters
    String columnNameProperty = config.getOrcColumns();
    columnNames = new ArrayList<>();
    if (columnNameProperty != null && columnNameProperty.length() > 0) {
      Collections.addAll(columnNames, columnNameProperty.split(String.valueOf(SerDeUtils.COMMA)));
    }
    columnNames.add(HoodieRecord.COMMIT_TIME_METADATA_FIELD);
    columnNames.add(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD);
    columnNames.add(HoodieRecord.RECORD_KEY_METADATA_FIELD);
    columnNames.add(HoodieRecord.PARTITION_PATH_METADATA_FIELD);
    columnNames.add(HoodieRecord.FILENAME_METADATA_FIELD);

    String columnTypeProperty = config.getOrcColumnsTypes();
    if (columnTypeProperty != null) {
      columnTypeProperty = columnTypeProperty + ",string,string,string,string,string";
    } else {
      // Default type: all string
      StringBuilder sb = new StringBuilder();
      for (int i = 0; i < columnNames.size(); i++) {
        if (i > 0) {
          sb.append(SerDeUtils.COMMA);
        }
        sb.append("string");
      }
      columnTypeProperty = sb.toString();
    }
    fieldTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);

    fieldObjectInspectors = new ArrayList<>(columnNames.size());
    for (TypeInfo typeInfo : fieldTypes) {
      fieldObjectInspectors.add(createObjectInspector(typeInfo));
    }
    hudiObjectInspector = ObjectInspectorFactory
        .getStandardStructObjectInspector(columnNames, fieldObjectInspectors);

    Configuration hudiConf = registerFileSystem(file, hadoopConfig);
    String  bloomFilterColumns = config.getOrcBloomFilterColumns();
    if (null != bloomFilterColumns) {
      bloomFilterColumns = bloomFilterColumns + String.valueOf(SerDeUtils.COMMA)
          + HoodieRecord.RECORD_KEY_METADATA_FIELD;
    } else {
      bloomFilterColumns = HoodieRecord.RECORD_KEY_METADATA_FIELD;
    }
    hudiConf.set(OrcTableProperties.BLOOM_FILTER_COLUMNS.name(), bloomFilterColumns);
    String bloomFilterFpp = config.getOrcBloomFilterFpp();
    hudiConf.set(OrcTableProperties.BLOOM_FILTER_FPP.name(), bloomFilterFpp);

    WriterOptions writerOptions = getOptions(hudiConf, config.getProps());
    writerOptions.inspector(hudiObjectInspector);
    writerOptions.stripeSize(orcStripeSize);
    writerOptions.blockSize(orcBlockSize);
    this.writer = OrcFile.createWriter(file, writerOptions);
  }

  public static Configuration registerFileSystem(Path file, Configuration conf) {
    Configuration returnConf = new Configuration(conf);
    String scheme = FSUtils.getFs(file.toString(), conf).getScheme();
    returnConf.set("fs." + HoodieWrapperFileSystem.getHoodieScheme(scheme) + ".impl",
        HoodieWrapperFileSystem.class.getName());
    return returnConf;
  }

  @Override
  public void writeAvroWithMetadata(R avroRecord, HoodieRecord record) throws IOException {
    GenericRecord genericRecord = (GenericRecord) avroRecord;
    String seqId = HoodieRecord.generateSequenceId(commitTime, TaskContext.getPartitionId(),
        recordIndex.getAndIncrement());

    HoodieAvroUtils.addHoodieKeyToRecord(genericRecord, record.getRecordKey(),
        record.getPartitionPath(), file.getName());

    GenericRecord metadataToRecord = HoodieAvroUtils
        .addCommitMetadataToRecord(genericRecord, commitTime, seqId);

    Schema avroSchema = metadataToRecord.getSchema();

    Object row = hudiObjectInspector.create();
    setOrcFiledValue((R) avroRecord, avroSchema, hudiObjectInspector, row);
    this.writer.addRow(row);
  }

  @Override
  public boolean canWrite() {
    return fs.getBytesWritten(file) < maxFileSize;
  }

  @Override
  public void writeAvro(String key, R avroRecord) throws IOException {
    Schema avroSchema = avroRecord.getSchema();
    Object row = hudiObjectInspector.create();
    setOrcFiledValue((R) avroRecord, avroSchema, hudiObjectInspector, row);
    this.writer.addRow(row);
  }

  @Override
  public void close() throws IOException {
    if (null != this.writer) {
      this.writer.close();
    }
  }

  private void setOrcFiledValue(R avroRecord, Schema avroSchema,
      StandardStructObjectInspector hudi, Object row) throws IOException {
    // Otherwise, return the row.
    for (int c = 0; c < columnNames.size(); c++) {
      try {
        // orc
        String fieldName = columnNames.get(c);
        TypeInfo typeInfo = fieldTypes.get(c);
        StructField structField = hudi.getAllStructFieldRefs().get(c);

        //avro
        Schema.Field field = avroSchema.getField(fieldName);
        Object fieldValue = avroRecord.get(field.pos());

        // Convert the column to the correct type when needed and set in row obj
        PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
        switch (pti.getPrimitiveCategory()) {
          case STRING:
            Text t = new Text(String.valueOf(fieldValue));
            hudi.setStructFieldData(row, structField, t);
            break;
          case BYTE:
            ByteWritable b = new ByteWritable(Byte.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, b);
            break;
          case SHORT:
            ShortWritable s = new ShortWritable(Short.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, s);
            break;
          case INT:
            IntWritable i = new IntWritable(Integer.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, i);
            break;
          case LONG:
            LongWritable l = new LongWritable(Long.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, l);
            break;
          case FLOAT:
            FloatWritable f = new FloatWritable(Float.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, f);
            break;
          case DOUBLE:
            DoubleWritable d = new DoubleWritable(Double.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, d);
            break;
          case BOOLEAN:
            BooleanWritable bool = new BooleanWritable(Boolean.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, bool);
            break;
          case TIMESTAMP:
            TimestampWritable ts = new TimestampWritable(Timestamp.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, ts);
            break;
          case DATE:
            DateWritable date = new DateWritable(Date.valueOf(String.valueOf(fieldValue)));
            hudi.setStructFieldData(row, structField, date);
            break;
          case DECIMAL:
            HiveDecimal db = HiveDecimal.create(String.valueOf(fieldValue));
            HiveDecimalWritable dbw = new HiveDecimalWritable(db);
            hudi.setStructFieldData(row, structField, dbw);
            break;
          case CHAR:
            HiveChar hc = new HiveChar(String.valueOf(fieldValue), ((CharTypeInfo) typeInfo).getLength());
            HiveCharWritable hcw = new HiveCharWritable(hc);
            hudi.setStructFieldData(row, structField, hcw);
            break;
          case VARCHAR:
            HiveVarchar hv = new HiveVarchar(String.valueOf(fieldValue), ((VarcharTypeInfo)typeInfo).getLength());
            HiveVarcharWritable hvw = new HiveVarcharWritable(hv);
            hudi.setStructFieldData(row, structField, hvw);
            break;
          default:
            throw new IOException("Unsupported type " + typeInfo);
        }
      } catch (RuntimeException e) {
        hudi.setStructFieldData(row, null, null);
      }
    }
  }

  private OrcFile.WriterOptions getOptions(Configuration conf, Properties props) {
    OrcFile.WriterOptions options = OrcFile.writerOptions(conf);
    String propVal;
    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.STRIPE_SIZE.getPropName(),
        props, conf)) != null) {
      options.stripeSize(Long.parseLong(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.COMPRESSION.getPropName(),
        props, conf)) != null) {
      options.compress(CompressionKind.valueOf(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.COMPRESSION_BLOCK_SIZE.getPropName(),
        props, conf)) != null) {
      options.bufferSize(Integer.parseInt(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.ROW_INDEX_STRIDE.getPropName(),
        props, conf)) != null) {
      options.rowIndexStride(Integer.parseInt(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.ENABLE_INDEXES.getPropName(),
        props, conf)) != null) {
      if ("false".equalsIgnoreCase(propVal)) {
        options.rowIndexStride(0);
      }
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.BLOCK_PADDING.getPropName(),
        props, conf)) != null) {
      options.blockPadding(Boolean.parseBoolean(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.ENCODING_STRATEGY.getPropName(),
        props, conf)) != null) {
      options.encodingStrategy(EncodingStrategy.valueOf(propVal));
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.BLOOM_FILTER_COLUMNS.getPropName(),
        props, conf)) != null) {
      options.bloomFilterColumns(propVal);
    }

    if ((propVal = getSettingFromPropsFallingBackToConf(OrcFile.OrcTableProperties.BLOOM_FILTER_FPP.getPropName(),
        props, conf)) != null) {
      options.bloomFilterFpp(Double.parseDouble(propVal));
    }

    return options;
  }

  /**
   * Helper method to get a parameter first from props if present, falling back to JobConf if not.
   * Returns null if key is present in neither.
   */
  private String getSettingFromPropsFallingBackToConf(String key, Properties props, Configuration conf) {
    if ((props != null) && props.containsKey(key)) {
      return props.getProperty(key);
    } else if (conf != null) {
      // If conf is not null, and the key is not present, Configuration.get() will
      // return null for us. So, we don't have to check if it contains it.
      return conf.get(key);
    } else {
      return null;
    }
  }

  static ObjectInspector createObjectInspector(TypeInfo typeInfo) {
    PrimitiveTypeInfo pti = (PrimitiveTypeInfo) typeInfo;
    switch (pti.getPrimitiveCategory()) {
      case FLOAT:
        return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
      case DOUBLE:
        return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
      case BOOLEAN:
        return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
      case BYTE:
        return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
      case SHORT:
        return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
      case INT:
        return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
      case LONG:
        return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
      case BINARY:
        return PrimitiveObjectInspectorFactory.writableBinaryObjectInspector;
      case STRING:
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
      case CHAR:
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(pti);
      case VARCHAR:
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(pti);
      case TIMESTAMP:
        return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
      case DATE:
        return PrimitiveObjectInspectorFactory.writableDateObjectInspector;
      case DECIMAL:
        return PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(pti);
      default:
        throw new IllegalArgumentException("Unknown primitive type " + pti.getPrimitiveCategory());
    }
  }
}
