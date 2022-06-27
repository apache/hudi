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

package org.apache.hudi.secondary.index.lucene;

import org.apache.hudi.common.config.HoodieBuildTaskConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieSecondaryIndexException;
import org.apache.hudi.secondary.index.SecondaryIndexBuilder;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.FloatPoint;
import org.apache.lucene.document.IntPoint;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.KeepOnlyLastCommitDeletionPolicy;
import org.apache.lucene.index.LogByteSizeMergePolicy;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class LuceneIndexBuilder implements SecondaryIndexBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexBuilder.class);

  private final String name;
  private final LinkedList<Schema.Field> indexFields;
  private final FSDirectory indexSaveDir;
  private IndexWriter indexWriter;
  private final Document reusedDoc = new Document();

  public LuceneIndexBuilder(HoodieBuildTaskConfig indexConfig) {
    this.name = "lucene-index-builder-" + System.nanoTime();
    this.indexFields = indexConfig.getIndexFields();
    try {
      this.indexSaveDir = FSDirectory.open(Paths.get(indexConfig.getIndexSaveDir()));
      IndexWriterConfig indexWriteConfig = getIndexWriteConfig(indexConfig);
      this.indexWriter = new IndexWriter(indexSaveDir, indexWriteConfig);
    } catch (IOException e) {
      throw new HoodieIOException("Init lucene index builder failed", e);
    }

    List<String> fieldNames = indexFields.stream()
        .map(Schema.Field::name)
        .collect(Collectors.toList());
    LOG.info("Init lucene index builder ok, name: {}, indexFields: {}", name, fieldNames);
  }

  @Override
  public void addBatch(GenericRecord[] records, int size) throws IOException {
    for (int i = 0; i < size; i++) {
      addRow(records[i]);
    }
  }

  @Override
  public void addRow(GenericRecord record) throws IOException {
    buildDocument(reusedDoc, record);
    indexWriter.addDocument(reusedDoc);
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public void close() {
    if (indexWriter != null) {
      try {
        indexWriter.forceMerge(1, true);
        indexWriter.close();
      } catch (IOException e) {
        LOG.error("Fail to close lucene index writer", e);
      }

      indexWriter = null;
    }
  }

  private void buildDocument(Document reusedDoc, GenericRecord record) {
    reusedDoc.clear();
    indexFields.forEach(avroField ->
        reusedDoc.add(createField(avroField, record.get(avroField.name()))));
  }

  private Field createField(Schema.Field avroField, Object value) {
    switch (avroField.schema().getType()) {
      case BOOLEAN:
        return new IntPoint(avroField.name(), (Boolean) value ? 1 : 0);
      case INT:
        return new IntPoint(avroField.name(), (Integer) value);
      case LONG:
        return new LongPoint(avroField.name(), (Long) value);
      case FLOAT:
        return new FloatPoint(avroField.name(), (Float) value);
      case DOUBLE:
        return new DoublePoint(avroField.name(), (Double) value);
      case STRING:
        byte[] bytes = ((Utf8) value).getBytes();
        return new StringField(avroField.name(),
            new BytesRef(bytes, 0, bytes.length), Field.Store.NO);
      case BYTES:
        return createFieldByAvroLogicalType(avroField);
      case NULL:
      default:
        throw new HoodieSecondaryIndexException(
            "Unsupported avro field type: " + avroField.schema().getType().getName());
    }
  }

  // Following code is forked {@code org.apache.avro.LogicalTypes}
  private static final String DECIMAL = "decimal";
  private static final String UUID = "uuid";
  private static final String DATE = "date";
  private static final String TIME_MILLIS = "time-millis";
  private static final String TIME_MICROS = "time-micros";
  private static final String TIMESTAMP_MILLIS = "timestamp-millis";
  private static final String TIMESTAMP_MICROS = "timestamp-micros";
  private static final String LOCAL_TIMESTAMP_MILLIS = "local-timestamp-millis";
  private static final String LOCAL_TIMESTAMP_MICROS = "local-timestamp-micros";

  private Field createFieldByAvroLogicalType(Schema.Field avroField) {
    switch (avroField.schema().getLogicalType().getName()) {
      case DECIMAL:
      case UUID:
        return new StringField(avroField.name(), "", Field.Store.NO);
      case DATE:
      case TIME_MILLIS:
      case TIME_MICROS:
      case TIMESTAMP_MILLIS:
      case TIMESTAMP_MICROS:
      case LOCAL_TIMESTAMP_MILLIS:
      case LOCAL_TIMESTAMP_MICROS:
        return new LongPoint(avroField.name(), 1);
      default:
        throw new HoodieSecondaryIndexException(
            "Unsupported avro logical field type: " + avroField.schema().getLogicalType().getName());
    }
  }

  /**
   * Convert hoodie build task config to lucene index writer config
   *
   * @param secondaryIndexConfig HoodieBuildTaskConfig
   * @return IndexWriterConfig
   */
  private IndexWriterConfig getIndexWriteConfig(HoodieBuildTaskConfig secondaryIndexConfig) {
    IndexWriterConfig config = new IndexWriterConfig();

    config.setUseCompoundFile(true);
    config.setCommitOnClose(true);
    config.setRAMBufferSizeMB(secondaryIndexConfig.getLuceneIndexRamBufferSizeMB());
    config.setIndexDeletionPolicy(new KeepOnlyLastCommitDeletionPolicy());

    ConcurrentMergeScheduler scheduler = new ConcurrentMergeScheduler();
    scheduler.setMaxMergesAndThreads(6, 1);
    config.setMergeScheduler(scheduler);

    LogByteSizeMergePolicy mergePolicy = new LogByteSizeMergePolicy();
    mergePolicy.setMergeFactor(secondaryIndexConfig.getLuceneIndexMergeFactor());
    mergePolicy.setMaxMergeMB(secondaryIndexConfig.getLuceneIndexMaxMergeMB());
    config.setMergePolicy(mergePolicy);
    config.setInfoStream(new LuceneIndexInfoStream(secondaryIndexConfig, name));

    try {
      if (DirectoryReader.indexExists(indexSaveDir)) {
        boolean success = indexSaveDir.getDirectory().toFile().delete();
        LOG.info("Delete index dir: {}", success);
      }
    } catch (IOException e) {
      throw new HoodieSecondaryIndexException(
          "Fail to delete lucene index dir: " + indexSaveDir.getDirectory().toFile().getPath(), e);
    }

    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    return config;
  }
}
