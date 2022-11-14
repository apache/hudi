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

package org.apache.hudi.hadoop;

import org.apache.hudi.FileSliceReader;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieAvroPayload;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodiePayloadProps;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableQueryType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.table.log.HoodieMergedLogRecordScanner;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hadoop.realtime.RealtimeSplit;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.io.storage.HoodieFileReader;

import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplitWithLocationInfo;
import org.apache.hadoop.mapred.JobConf;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.hadoop.utils.HoodieRealtimeRecordReaderUtils.getMergedLogRecordScanner;
import static org.apache.hudi.io.storage.HoodieFileReaderFactory.getReaderFactory;

public abstract class HoodieHiveFileSliceReader implements FileSliceReader {

  private static final Logger LOG = LogManager.getLogger(HoodieHiveFileSliceReader.class);

  private final FileSplit split;
  private final JobConf jobConf;
  private final HoodieTableMetaClient metaClient;
  private final Properties payloadProps = new Properties();

  // table schema from metadata
  private final Schema schema;

  // other schema handles
  private Schema readerSchema;
  private Schema writerSchema;

  private String readMode;

  public HoodieHiveFileSliceReader(InputSplitWithLocationInfo split, JobConf jobConf) {
    this.split = (FileSplit) split;
    this.jobConf = jobConf;
    HoodiePartitionMetadata metadata = null;
    try {
      Path path = this.split.getPath();
      metadata = new HoodiePartitionMetadata(path.getFileSystem(jobConf), path);
      metadata.readFromFS();
      String basePath = String.valueOf(HoodieHiveUtils.getNthParent(path, metadata.getPartitionDepth()));
      this.metaClient = HoodieTableMetaClient.builder().setConf(jobConf).setBasePath(basePath).build();
      this.payloadProps.setProperty(HoodiePayloadProps.PAYLOAD_ORDERING_FIELD_PROP_KEY, metaClient.getTableConfig().getPreCombineField());
      this.schema = new TableSchemaResolver(metaClient).getTableAvroSchema();
    } catch (IOException e) {
      throw new HoodieIOException("Failed to initialize metaclient for split: " + this.split);
    } catch (Exception e) {
      throw new HoodieException("Failed to get table avro schema.");
    }
  }

  @Override
  public FileSliceReader project(InternalSchema schema) {
    // should we convert to avro Schema?
    // TODO: implement me
    return this;
  }

  @Override
  public FileSliceReader project(Schema requiredSchema) {
    String partitionFields = jobConf.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "");
    List<String> partitioningFields =
        partitionFields.length() > 0 ? Arrays.stream(partitionFields.split("/")).collect(Collectors.toList())
            : new ArrayList<>();
    this.writerSchema = HoodieRealtimeRecordReaderUtils.addPartitionFields(schema, partitioningFields);

    List<String> projectionFields = HoodieRealtimeRecordReaderUtils.orderFields(jobConf.get(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR),
        jobConf.get(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR), partitioningFields);
    Map<String, Schema.Field> schemaFieldsMap = HoodieRealtimeRecordReaderUtils.getNameToFieldMap(writerSchema);
    this.readerSchema = HoodieRealtimeRecordReaderUtils.generateProjectionSchema(writerSchema, schemaFieldsMap, projectionFields);

    return this;
  }

  @Override
  public FileSliceReader pushDownFilters(Set<String> filters) {
    String tableName = metaClient.getTableConfig().getTableName();

    if (HoodieTableQueryType.INCREMENTAL.name().equals(readMode)) {
      // TODO: construct predicate
      // FilterPredicate predicate = constructHoodiePredicate(jobConf, tableName, split);
      // LOG.info("Setting parquet predicate push down as " + predicate);
      // ParquetInputFormat.setFilterPredicate(jobConf, predicate);
    }

    return this;
  }

  @Override
  public FileSliceReader readingMode(HoodieTableQueryType queryType) {
    this.readMode = queryType.name();
    return this;
  }

  @Override
  public Iterator<HoodieRecord> open(FileSlice fileSlice) {
    try (HoodieFileReader baseFileReader = getReaderFactory(HoodieRecord.HoodieRecordType.HIVE).getFileReader(jobConf, split.getPath())) {
      Iterator<HoodieRecord> baseRecordIterator = baseFileReader.getRecordIterator(readerSchema);
      if (split instanceof RealtimeSplit) {
        HoodieMergedLogRecordScanner logRecordScanner = getMergedLogRecordScanner((RealtimeSplit) split, jobConf, readerSchema);
        while (baseRecordIterator.hasNext()) {
          logRecordScanner.processNextRecord(baseRecordIterator.next());
        }
        return logRecordScanner.iterator();
      } else {
        return baseRecordIterator;
      }
    } catch (IOException e) {
      throw new HoodieIOException("Failed to open file slice: " + fileSlice);
    }
  }

  @Override
  public void close() throws Exception {

  }

  public Schema getReaderSchema() {
    return readerSchema;
  }

  public Schema getWriterSchema() {
    return writerSchema;
  }

  public FileSplit getSplit() {
    return split;
  }

  public JobConf getJobConf() {
    return jobConf;
  }

  public boolean useCustomPayload() {
    return !(metaClient.getTableConfig().getPayloadClass().contains(HoodieAvroPayload.class.getName())
        || metaClient.getTableConfig().getPayloadClass().contains("org.apache.hudi.OverwriteWithLatestAvroPayload"));
  }

  public Properties getPayloadProps() {
    return payloadProps;
  }
}
