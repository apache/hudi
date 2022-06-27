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

package org.apache.hudi.table.action.build;

import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieBuildTask;
import org.apache.hudi.common.config.HoodieBuildTaskConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.config.SerializableSchema;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BuildStatus;
import org.apache.hudi.common.util.BuildUtils;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.exception.HoodieBuildException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieFileReaderFactory;
import org.apache.hudi.secondary.index.HoodieSecondaryIndex;
import org.apache.hudi.secondary.index.SecondaryIndexBuilder;
import org.apache.hudi.secondary.index.SecondaryIndexFactory;
import org.apache.hudi.secondary.index.SecondaryIndexType;
import org.apache.hudi.secondary.index.SecondaryIndexUtils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BuildTaskExecutor {
  private static final Logger LOG = LoggerFactory.getLogger(BuildTaskExecutor.class);

  private final String partition;
  private final Path baseFilePath;
  private final String indexFolderPath;
  private final List<HoodieSecondaryIndex> combinedSecondaryIndices;
  private final List<SecondaryIndexBuilder> indexBuilders;
  private final ClosableIterator<IndexedRecord> recordIterator;

  private final int batchSize = 1000;
  private final GenericRecord[] reusedRecords;

  private long totalRecords = 0;
  private final HoodieTimer buildTimer;

  public BuildTaskExecutor(HoodieBuildTask buildTask,
                           String basePath,
                           String indexFolderPath,
                           SerializableSchema schema,
                           SerializableConfiguration conf) {
    this.partition = buildTask.getPartition();
    this.baseFilePath = new Path(buildTask.getBaseFilePath());
    this.indexFolderPath = indexFolderPath;

    Schema avroSchema = schema.get();
    Configuration hadoopConf = conf.get();

    List<HoodieSecondaryIndex> secondaryIndices = SecondaryIndexUtils.fromJsonString(buildTask.getIndexMetas());
    combinedSecondaryIndices = combineSecondaryIndices(secondaryIndices);
    // Init index builder for every combined secondary indices
    this.indexBuilders = combinedSecondaryIndices.stream().map(indexInfo ->
        initBuilderForIndex(indexInfo, avroSchema, hadoopConf)
    ).collect(Collectors.toList());

    // Create record iterator for this base file
    Schema readerSchema = HoodieAvroUtils.addMetadataFields(avroSchema);
    try {
      Path baseFileFullPath = new Path(basePath, buildTask.getBaseFilePath());
      recordIterator = HoodieFileReaderFactory.getFileReader(hadoopConf, baseFileFullPath)
          .getRecordIterator(readerSchema);
    } catch (IOException e) {
      throw new HoodieBuildException("Fail to get record iterator for: " + baseFilePath, e);
    }

    reusedRecords = new GenericRecord[batchSize];
    buildTimer = new HoodieTimer();

    LOG.info("Init build task executor ok, basePath:{}, file: {}, indexSaveDir: {}, batchSize: {}",
        basePath, baseFilePath, indexFolderPath, batchSize);
  }

  public BuildStatus execute() {
    buildTimer.startTimer();
    int recordNum = 0;
    while (recordIterator.hasNext()) {
      GenericRecord next = (GenericRecord) recordIterator.next();
      reusedRecords[recordNum++] = next;

      if (recordNum == batchSize) {
        addBatch(reusedRecords, recordNum);
        totalRecords += recordNum;
        recordNum = 0;
      }
    }

    if (recordNum != 0) {
      addBatch(reusedRecords, recordNum);
      totalRecords += recordNum;
    }

    // Close all index builders
    indexBuilders.forEach(SecondaryIndexBuilder::close);
    LOG.info("Finish building indexes for file: {}, timeCost: {}ms",
        baseFilePath, buildTimer.endTimer());

    return BuildStatus.builder()
        .setPartition(partition)
        .setBaseFilePath(baseFilePath.toString())
        .setTotalRecords(totalRecords)
        .setSecondaryIndexes(combinedSecondaryIndices)
        .build();
  }

  /**
   * Different fields to build lucene secondary index can be combined as a single lucene
   * secondary index to reduce index data files and simplify the query logic. So here we
   * try to combine multi lucene secondary indices to one lucene secondary index, the
   * index name of combined lucene secondary index is the first lucene secondary index,
   * and combined lucene secondary index has all origin lucene secondary indices' options.
   *
   * @param secondaryIndices All secondary indices need build for this file
   * @return Combined secondary indices
   */
  private List<HoodieSecondaryIndex> combineSecondaryIndices(List<HoodieSecondaryIndex> secondaryIndices) {
    List<HoodieSecondaryIndex> combinedSecondaryIndices = new ArrayList<>();
    Map<SecondaryIndexType, List<HoodieSecondaryIndex>> groupedIndices =
        secondaryIndices.stream().collect(Collectors.groupingBy(HoodieSecondaryIndex::getIndexType));
    List<HoodieSecondaryIndex> luceneIndices = groupedIndices.get(SecondaryIndexType.LUCENE);
    if (!CollectionUtils.isNullOrEmpty(luceneIndices)) {
      LinkedHashMap<String, Map<String, String>> columns = new LinkedHashMap<>();
      Map<String, String> options = new HashMap<>();
      luceneIndices.forEach(index -> {
        columns.putAll(index.getColumns());
        options.putAll(index.getOptions());
      });

      HoodieSecondaryIndex secondaryIndex = HoodieSecondaryIndex.builder()
          .setIndexName(luceneIndices.get(0).getIndexName())
          .setColumns(columns)
          .setIndexType(SecondaryIndexType.LUCENE.name())
          .setOptions(options)
          .build();

      combinedSecondaryIndices.add(secondaryIndex);
      groupedIndices.remove(SecondaryIndexType.LUCENE);
    }
    groupedIndices.values().forEach(combinedSecondaryIndices::addAll);

    return combinedSecondaryIndices;
  }

  /**
   * Init index builder for given secondary index
   *
   * @param secondaryIndex HoodieSecondaryIndex
   * @param schema         Avro schema for this table
   * @param conf           Hadoop conf
   * @return IndexBuilder
   */
  private SecondaryIndexBuilder initBuilderForIndex(
      HoodieSecondaryIndex secondaryIndex,
      Schema schema,
      Configuration conf) {
    SecondaryIndexType indexType = secondaryIndex.getIndexType();

    LinkedList<Schema.Field> indexFields = new LinkedList<>();
    secondaryIndex.getColumns().forEach((colName, options) -> {
      Schema.Field field = schema.getField(colName);
      ValidationUtils.checkArgument(field != null,
          "Field not exists: " + colName + ", schema: " + schema);
      indexFields.add(field);
    });

    Path indexSaveDir = BuildUtils.getIndexSaveDir(indexFolderPath, indexType.name(), baseFilePath.getName());
    try {
      FileSystem fs = FSUtils.getFs(indexSaveDir, conf);
      if (!fs.exists(indexSaveDir)) {
        fs.mkdirs(indexSaveDir);
        LOG.info("Create index save dir ok: {}", indexSaveDir);
      }
    } catch (IOException e) {
      throw new HoodieBuildException("Fail to make index save dir");
    }

    HoodieBuildTaskConfig config = HoodieBuildTaskConfig.builder()
        .setIndexSaveDir(indexSaveDir.toString())
        .setIndexType(indexType)
        .setIndexFields(indexFields)
        .build();

    return SecondaryIndexFactory.getIndexBuilder(config);
  }

  /**
   * Batch add records to index builders
   *
   * @param records Records to be added
   * @param size    The size of records
   */
  private void addBatch(GenericRecord[] records, int size) {
    indexBuilders.forEach(indexBuilder -> {
      try {
        indexBuilder.addBatch(records, size);
      } catch (IOException e) {
        throw new HoodieIOException("Add records to index builder failed, baseFile: "
            + baseFilePath + ", builderName: " + indexBuilder.getName(), e);
      }
    });
  }
}
