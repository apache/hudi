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

package org.apache.hudi.source.stats;

import org.apache.hudi.client.common.HoodieFlinkEngineContext;
import org.apache.hudi.common.data.HoodieListData;
import org.apache.hudi.common.data.HoodiePairData;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieRecordGlobalLocation;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieDataUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.OptionsResolver;
import org.apache.hudi.keygen.KeyGenUtils;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.sink.bulk.RowDataKeyGen;
import org.apache.hudi.source.ExpressionEvaluators;
import org.apache.hudi.util.StreamerUtil;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * An index support implementation that leverages Record Level Index to prune file slices.
 */
public class RecordLevelIndex implements FlinkMetadataIndex {
  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LoggerFactory.getLogger(RecordLevelIndex.class);

  private final String basePath;
  private final Configuration conf;
  private final List<String> hoodieKeysFromFilter;
  private final HoodieTableMetaClient metaClient;
  private HoodieTableMetadata metadataTable;

  private RecordLevelIndex(
      String basePath,
      Configuration conf,
      HoodieTableMetaClient metaClient,
      List<String> hoodieKeysFromFilter) {
    this.basePath = basePath;
    this.conf = conf;
    this.metaClient = metaClient;
    this.hoodieKeysFromFilter = hoodieKeysFromFilter;
  }

  @Override
  public String getIndexPartitionName() {
    return HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX;
  }

  @Override
  public boolean isIndexAvailable() {
    return metaClient.getTableConfig().isMetadataTableAvailable()
        && metaClient.getTableConfig().getMetadataPartitions().contains(HoodieTableMetadataUtil.PARTITION_NAME_RECORD_INDEX);
  }

  public HoodieTableMetadata getMetadataTable() {
    // initialize the metadata table lazily
    if (this.metadataTable == null) {
      this.metadataTable = metaClient.getTableFormat().getMetadataFactory().create(
          HoodieFlinkEngineContext.DEFAULT,
          metaClient.getStorage(),
          StreamerUtil.metadataConfig(conf),
          basePath);
    }
    return this.metadataTable;
  }

  public List<FileSlice> computeCandidateFileSlices(List<FileSlice> fileSlices) {
    if (!isIndexAvailable()) {
      return fileSlices;
    }
    HoodiePairData<String, HoodieRecordGlobalLocation> recordIndexData =
        getMetadataTable().readRecordIndexLocationsWithKeys(HoodieListData.eager(hoodieKeysFromFilter));
    try {
      List<Pair<String, HoodieRecordGlobalLocation>> recordIndexLocations = HoodieDataUtils.dedupeAndCollectAsList(recordIndexData);
      Set<String> fileIds = recordIndexLocations.stream()
          .map(pair -> pair.getValue().getFileId()).collect(Collectors.toSet());
      return fileSlices.stream().filter(fileSlice -> fileIds.contains(fileSlice.getFileId())).collect(Collectors.toList());
    } finally {
      // Clean up the RDD to avoid memory leaks
      recordIndexData.unpersistWithDependencies();
    }
  }

  public static Option<RecordLevelIndex> create(
      String basePath,
      Configuration conf,
      HoodieTableMetaClient metaClient,
      List<ExpressionEvaluators.Evaluator> evaluators,
      RowType rowType) {
    if (evaluators.isEmpty() || !FlinkOptions.QUERY_TYPE_SNAPSHOT.equalsIgnoreCase(conf.get(FlinkOptions.QUERY_TYPE))) {
      return Option.empty();
    }
    if (metaClient == null) {
      metaClient = StreamerUtil.createMetaClient(conf);
    }
    if (KeyGenUtils.mayUseNewEncodingForComplexKeyGen(metaClient.getTableConfig())) {
      return Option.empty();
    }

    String[] recordKeyFields = metaClient.getTableConfig().getRecordKeyFields().orElse(new String[0]);
    if (recordKeyFields.length == 0) {
      LOG.warn("The table do not have record keys, skipping the rli pruning.");
      return Option.empty();
    }
    boolean consistentLogicalTimestampEnabled = OptionsResolver.isConsistentLogicalTimestampEnabled(conf);
    List<String> hoodieKeysFromFilter = computeHoodieKeyFromFilters(conf, metaClient, evaluators, recordKeyFields, rowType, consistentLogicalTimestampEnabled);
    if (hoodieKeysFromFilter.isEmpty()) {
      LOG.warn("The number of keys from query predicate is empty, skipping the rli pruning.");
      return Option.empty();
    }
    int maxKeyNum = conf.get(FlinkOptions.READ_DATA_SKIPPING_RLI_KEYS_MAX_NUM);
    if (hoodieKeysFromFilter.size() > maxKeyNum) {
      LOG.warn("The number of keys from query predicate: {} exceeds the upper threshold: {}, skipping the rli pruning, the keys: {}",
          hoodieKeysFromFilter.size(), maxKeyNum, hoodieKeysFromFilter);
      return Option.empty();
    }
    return Option.of(new RecordLevelIndex(basePath, conf, metaClient, hoodieKeysFromFilter));
  }

  /**
   * Given query filters, it filters the EqualTo, IN and OR queries on record key columns and
   * returns the list of record key literals present in the query, for example:
   * <p>
   * filter1: `key1` = 'val1', returns {"val1"}
   * filter2: `key1` in ('val1', 'val2', 'val3'), returns {"val1", "vale", "val3"}
   * filter3: `key1` = 'val1' OR `key1` = 'val2' or `key1` = 'val3', returns {"val1", "vale", "val3"}
   * filter4: `key1` = 'val1' AND `key2` in ('val2', 'val3'), returns {"key1:val1,key2:val2", "key1:val1,key2:val3"}
   */
  @VisibleForTesting
  public static List<String> computeHoodieKeyFromFilters(
      Configuration conf,
      HoodieTableMetaClient metaClient,
      List<ExpressionEvaluators.Evaluator> evaluators,
      String[] keyFields,
      RowType rowType,
      boolean consistentLogicalTimestampEnabled) {
    String[] partitionFields = metaClient.getTableConfig().getPartitionFields().orElse(new String[0]);
    // align with the check logic in RowDataKeyGen
    boolean isComplexRecordKey = keyFields.length > 1 || partitionFields.length > 1 && !OptionsResolver.useComplexKeygenNewEncoding(conf);
    List<String> hoodieKeys = new ArrayList<>();
    List<String> fieldNames = rowType.getFieldNames();
    for (String keyField: keyFields) {
      List<String> recordKeys = new ArrayList<>();
      LogicalType fieldType = rowType.getTypeAt(fieldNames.indexOf(keyField));
      for (ExpressionEvaluators.Evaluator evaluator: evaluators) {
        // if there exists multiple ref fields in an evaluator, ignore this evaluator, e.g., key = 'key1' or age = 20
        List<Object> literals = collectLiterals(evaluator, keyField);
        literals.forEach(val -> recordKeys.add(isComplexRecordKey
            ? keyField + KeyGenerator.DEFAULT_COLUMN_VALUE_SEPARATOR + normalizeLiteral(val, keyField, fieldType, consistentLogicalTimestampEnabled)
            : normalizeLiteral(val, keyField, fieldType, consistentLogicalTimestampEnabled)));
      }
      if (recordKeys.isEmpty()) {
        LOG.info("No literals found for the record key: {}, therefore filtering can not be performed", keyField);
        return Collections.emptyList();
      } else if (!isComplexRecordKey || hoodieKeys.isEmpty()) {
        hoodieKeys = recordKeys;
      } else {
        // Combine literals for this configured record key with literals for the other configured record keys
        // If there are two literals for rk1, rk2, rk3 each. A total of 8 combinations will be generated
        List<String> tmpHoodieKeys = new ArrayList<>();
        for (String compositeKey: hoodieKeys) {
          for (String recordKey: recordKeys) {
            tmpHoodieKeys.add(compositeKey + KeyGenerator.DEFAULT_RECORD_KEY_PARTS_SEPARATOR + recordKey);
          }
        }
        hoodieKeys = tmpHoodieKeys;
      }
    }
    return hoodieKeys;
  }

  /**
   * Collect literal values for record key fields from the predicate.
   */
  private static List<Object> collectLiterals(ExpressionEvaluators.Evaluator evaluator, String refName) {
    if (evaluator instanceof ExpressionEvaluators.LeafEvaluator
        && !((ExpressionEvaluators.LeafEvaluator) evaluator).getName().equalsIgnoreCase(refName)) {
      return Collections.emptyList();
    }
    if (evaluator instanceof ExpressionEvaluators.EqualTo) {
      Object valueLiteral = ((ExpressionEvaluators.EqualTo) evaluator).getVal();
      return valueLiteral == null ? Collections.emptyList() : Collections.singletonList(valueLiteral);
    } else if (evaluator instanceof ExpressionEvaluators.In) {
      Object[] valueLiterals = ((ExpressionEvaluators.In) evaluator).getVals();
      if (valueLiterals.length < 1 || Arrays.stream(valueLiterals).anyMatch(Objects::isNull)) {
        return Collections.emptyList();
      }
      return Arrays.stream(valueLiterals).collect(Collectors.toList());
    } else if (evaluator instanceof ExpressionEvaluators.Or) {
      List<List<Object>> literalsList = Arrays.stream(((ExpressionEvaluators.Or) evaluator).getEvaluators())
          .map(eval -> collectLiterals(eval, refName)).collect(Collectors.toList());
      // if any child expr do not contain predicate on the key, just return empty list
      if (literalsList.stream().anyMatch(List::isEmpty)) {
        return Collections.emptyList();
      }
      return literalsList.stream().flatMap(List::stream).distinct().collect(Collectors.toList());
    } else {
      return Collections.emptyList();
    }
  }

  /**
   * Normalize literal values before used to get record index locations.
   */
  private static String normalizeLiteral(Object value, String keyField, LogicalType fieldType, boolean consistentLogicalTimestampEnabled) {
    switch (fieldType.getTypeRoot()) {
      case DECIMAL:
        // the scale of decimal data in predicate may not be aligned with that in record index, padding 0 if necessary,
        // e.g., 1.11 with target scale 5, return 1.11000
        BigDecimal decimal = (BigDecimal) value;
        int targetScale = ((DecimalType) fieldType).getScale();
        value = decimal.scale() >= targetScale ? value : decimal.setScale(targetScale);
        break;
      case TIMESTAMP_WITHOUT_TIME_ZONE:
        // the original value is extracted from literal by ExpressionUtils#getValueFromLiteral, which is epoch millis
        // convert it back to TimestampData before reusing key generating logic in RowDataKeyGen.
        value = TimestampData.fromEpochMillis((Long) value);
        break;
      default:
        break;
    }
    // to align with the hoodie key generating logic in writer side.
    return RowDataKeyGen.getRecordKey(value, keyField, consistentLogicalTimestampEnabled);
  }
}
