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
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieBuildException;
import org.apache.hudi.exception.HoodieSecondaryIndexException;
import org.apache.hudi.internal.schema.Type;
import org.apache.hudi.internal.schema.convert.AvroInternalSchemaConverter;
import org.apache.hudi.secondary.index.ISecondaryIndexBuilder;
import org.apache.hudi.secondary.index.lucene.hadoop.HdfsDirectory;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.BytesRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static org.apache.hudi.secondary.index.IndexConstants.NOT_NULL_FIELD;
import static org.apache.hudi.secondary.index.IndexConstants.NULL_FIELD;

public class LuceneIndexBuilder implements ISecondaryIndexBuilder {
  private static final Logger LOG = LoggerFactory.getLogger(LuceneIndexBuilder.class);

  private final String name;
  private final LinkedList<Schema.Field> indexFields;
  private final Configuration conf;
  private final Type.TypeID[] fieldTypes;
  private final String indexSaveDir;
  private final Directory directory;
  private IndexWriter indexWriter;
  private final Document reusedDoc = new Document();

  public LuceneIndexBuilder(HoodieBuildTaskConfig indexConfig) {
    this.name = "lucene-index-builder-" + System.nanoTime();
    this.indexFields = indexConfig.getIndexFields();
    this.conf = indexConfig.getConf();
    this.indexSaveDir = indexConfig.getIndexSaveDir();
    try {
      Path path = new Path(indexConfig.getIndexSaveDir());
      String scheme = path.toUri().getScheme();
      if (!StringUtils.isNullOrEmpty(scheme)) {
        String disableCacheName = String.format("fs.%s.impl.disable.cache", scheme);
        conf.set(disableCacheName, "true");
      }
      this.directory = new HdfsDirectory(path, conf);
      IndexWriterConfig indexWriteConfig = getIndexWriteConfig(indexConfig);
      this.indexWriter = new IndexWriter(directory, indexWriteConfig);
    } catch (Exception e) {
      throw new HoodieBuildException("Init lucene index builder failed", e);
    }

    List<String> fieldNames = new ArrayList<>();
    fieldTypes = new Type.TypeID[indexFields.size()];
    IntStream.range(0, indexFields.size()).forEach(i -> {
      Schema.Field field = indexFields.get(i);
      fieldTypes[i] = AvroInternalSchemaConverter.buildTypeFromAvroSchema(field.schema()).typeId();
      fieldNames.add(field.name());
    });
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
    IntStream.range(0, indexFields.size()).forEach(i -> {
      String fieldName = indexFields.get(i).name();
      Object fieldValue = record.get(fieldName);
      if (Objects.isNull(fieldValue)) {
        reusedDoc.add(new StringField(NULL_FIELD, fieldName, Field.Store.NO));
      } else {
        reusedDoc.add(createField(fieldName, fieldTypes[i], fieldValue));
        reusedDoc.add(new StringField(NOT_NULL_FIELD, fieldName, Field.Store.NO));
      }
    });
  }

  /**
   * Build lucene field from given field name, field type, and field value
   *
   * @param fieldName Field name
   * @param typeID    Data type
   * @param value     Field value
   * @return Lucene field
   */
  private Field createField(String fieldName, Type.TypeID typeID, Object value) {
    ValidationUtils.checkArgument(value != null);
    switch (typeID) {
      case BOOLEAN:
        return new IntPoint(fieldName, (Boolean) value ? 1 : 0);
      case INT:
        return new IntPoint(fieldName, (Integer) value);
      case LONG:
      case DATE:
      case TIME:
      case TIMESTAMP:
        return new LongPoint(fieldName, (Long) value);
      case FLOAT:
        return new FloatPoint(fieldName, (Float) value);
      case DOUBLE:
        return new DoublePoint(fieldName, (Double) value);
      case STRING:
      case BINARY:
      case UUID:
      case DECIMAL:
        byte[] bytes = ((Utf8) value).getBytes();
        return new StringField(fieldName,
            new BytesRef(bytes, 0, bytes.length), Field.Store.NO);
      default:
        throw new HoodieSecondaryIndexException("Unsupported field type: " + typeID.getName());
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
      if (DirectoryReader.indexExists(directory)) {
        FSUtils.getFs(indexSaveDir, conf).delete(new Path(indexSaveDir), true);
        LOG.info("Delete index dir: {}", indexSaveDir);
      }
    } catch (IOException e) {
      throw new HoodieSecondaryIndexException("Fail to delete lucene index dir: " + indexSaveDir, e);
    }

    config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);

    return config;
  }
}
