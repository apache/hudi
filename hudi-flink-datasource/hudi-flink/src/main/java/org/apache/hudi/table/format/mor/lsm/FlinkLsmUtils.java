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

package org.apache.hudi.table.format.mor.lsm;

import org.apache.hudi.client.model.CommitTimeFlinkRecordMerger;
import org.apache.hudi.client.model.HoodieFlinkRecord;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieAvroRecordMerger;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.log.InstantRange;
import org.apache.hudi.common.util.ClosableIterator;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieCleanConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.configuration.FlinkOptions;
import org.apache.hudi.configuration.HadoopConfigurations;
import org.apache.hudi.io.lsm.RecordReader;
import org.apache.hudi.index.HoodieIndex;
import org.apache.hudi.source.ExpressionPredicates;
import org.apache.hudi.table.action.cluster.strategy.LsmBaseClusteringPlanStrategy;
import org.apache.hudi.table.format.FilePathUtils;
import org.apache.hudi.table.format.InternalSchemaManager;
import org.apache.hudi.table.format.RecordIterators;
import org.apache.hudi.util.DataTypeUtils;

import org.apache.avro.Schema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.io.disk.iomanager.IOManager;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.expressions.CallExpression;
import org.apache.flink.table.expressions.FieldReferenceExpression;
import org.apache.flink.table.expressions.ValueLiteralExpression;
import org.apache.flink.table.functions.BuiltInFunctionDefinitions;
import org.apache.flink.table.functions.FunctionIdentifier;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.hudi.common.model.HoodieRecordMerger.COMMIT_TIME_BASED_MERGE_STRATEGY_UUID;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.HOODIE_RECORD_KEY_COL_POS;

public class FlinkLsmUtils {

  private static final Logger LOG = LoggerFactory.getLogger(FlinkLsmUtils.class);

  public static Comparator<HoodieRecord> getHoodieFlinkRecordComparator() {
    return new Comparator<HoodieRecord>() {
      @Override
      public int compare(HoodieRecord o1, HoodieRecord o2) {
        HoodieFlinkRecord record1 = (HoodieFlinkRecord) o1;
        HoodieFlinkRecord record2 = (HoodieFlinkRecord) o2;
        return record1.getRecordKeyStringData().compareTo(record2.getRecordKeyStringData());
      }
    };
  }

  public static int[] getLsmRequiredPositions(Schema requiredSchemaWithMeta, Schema tableSchema) {
    List<String> fieldNames = tableSchema.getFields().stream().map(Schema.Field::name).collect(Collectors.toList());
    return requiredSchemaWithMeta.getFields().stream().map(Schema.Field::name).map(fieldNames::indexOf)
        .mapToInt(i -> i)
        .toArray();
  }

  public static ClosableIterator<RowData> getBaseFileIterator(String path,
                                                              int[] requiredPos,
                                                              List<String> fieldNames,
                                                              List<DataType> fieldTypes,
                                                              String defaultPartName,
                                                              Configuration conf,
                                                              org.apache.hadoop.conf.Configuration hadoopConf,
                                                              HoodieTableConfig tableConfig,
                                                              InternalSchemaManager internalSchemaManager,
                                                              List<ExpressionPredicates.Predicate> predicates) throws IOException {
    // generate partition specs.
    LinkedHashMap<String, String> partSpec = FilePathUtils.extractPartitionKeyValues(
        new org.apache.hadoop.fs.Path(path).getParent(),
        conf.getBoolean(FlinkOptions.HIVE_STYLE_PARTITIONING),
        FilePathUtils.extractPartitionKeys(conf));
    LinkedHashMap<String, Object> partObjects = new LinkedHashMap<>();
    partSpec.forEach((k, v) -> {
      final int idx = fieldNames.indexOf(k);
      if (idx == -1) {
        // for any rare cases that the partition field does not exist in schema,
        // fallback to file read
        return;
      }
      DataType fieldType = fieldTypes.get(idx);
      if (!DataTypeUtils.isDatetimeType(fieldType)) {
        // date time type partition field is formatted specifically,
        // read directly from the data file to avoid format mismatch or precision loss
        partObjects.put(k, DataTypeUtils.resolvePartition(defaultPartName.equals(v) ? null : v, fieldType));
      }
    });
    org.apache.hadoop.conf.Configuration parquetConf = HadoopConfigurations.getParquetConf(conf, hadoopConf);
    FSUtils.addChubaoFsConfig2HadoopConf(parquetConf, tableConfig);
    return RecordIterators.getParquetRecordIterator(
        internalSchemaManager,
        conf.getBoolean(FlinkOptions.UTC_TIMEZONE),
        true,
        parquetConf,
        fieldNames.toArray(new String[0]),
        fieldTypes.toArray(new DataType[0]),
        partObjects,
        requiredPos,
        2048,
        new org.apache.flink.core.fs.Path(path),
        0,
        Long.MAX_VALUE, // read the whole file
        predicates);
  }

  public static CallExpression buildCloseCloseRangeCommitExpression(InstantRange instantRange) {
    return new CallExpression(
        FunctionIdentifier.of("AND"),
        BuiltInFunctionDefinitions.AND,
        Arrays.asList(
            new CallExpression(
                FunctionIdentifier.of("greaterThanOrEqual"),
                BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
                Arrays.asList(
                    new FieldReferenceExpression(
                        HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                        DataTypes.STRING(),
                        0,
                        HOODIE_RECORD_KEY_COL_POS
                    ),
                    new ValueLiteralExpression(instantRange.getStartInstant())
                ),
                DataTypes.BOOLEAN()
            ),
            new CallExpression(
                FunctionIdentifier.of("lessThanOrEqual"),
                BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                Arrays.asList(
                    new FieldReferenceExpression(
                        HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                        DataTypes.STRING(),
                        0,
                        HOODIE_RECORD_KEY_COL_POS
                    ),
                    new ValueLiteralExpression(instantRange.getEndInstant())
                ),
                DataTypes.BOOLEAN()
            )
        ),
        DataTypes.BOOLEAN()
    );
  }

  public static CallExpression buildCloseCloseRangeNullableBoundaryCommitExpression(InstantRange instantRange) {
    if (instantRange.getStartInstant() == null) {
      return new CallExpression(
          FunctionIdentifier.of("lessThanOrEqual"),
          BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
          Arrays.asList(
              new FieldReferenceExpression(
                  HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                  DataTypes.STRING(),
                  0,
                  HOODIE_RECORD_KEY_COL_POS
              ),
              new ValueLiteralExpression(instantRange.getEndInstant())
          ),
          DataTypes.BOOLEAN()
      );
    } else if (instantRange.getEndInstant() == null) {
      return new CallExpression(
          FunctionIdentifier.of("greaterThanOrEqual"),
          BuiltInFunctionDefinitions.GREATER_THAN_OR_EQUAL,
          Arrays.asList(
              new FieldReferenceExpression(
                  HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                  DataTypes.STRING(),
                  0,
                  HOODIE_RECORD_KEY_COL_POS
              ),
              new ValueLiteralExpression(instantRange.getStartInstant())
          ),
          DataTypes.BOOLEAN()
      );
    } else {
      return buildCloseCloseRangeCommitExpression(instantRange);
    }
  }

  public static CallExpression buildOpenCloseRangeCommitExpression(InstantRange instantRange) {
    return new CallExpression(
        FunctionIdentifier.of("greaterThan"),
        BuiltInFunctionDefinitions.GREATER_THAN,
        Arrays.asList(
            new FieldReferenceExpression(
                HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                DataTypes.STRING(),
                0,
                HOODIE_RECORD_KEY_COL_POS
            ),
            new ValueLiteralExpression(instantRange.getStartInstant())
        ),
        DataTypes.BOOLEAN()
    );
  }

  public static CallExpression buildOpenCloseRangeNullableBoundaryCommitExpression(InstantRange instantRange) {
    if (instantRange.getStartInstant() == null) {
      return new CallExpression(
          FunctionIdentifier.of("lessThanOrEqual"),
          BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
          Arrays.asList(
              new FieldReferenceExpression(
                  HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                  DataTypes.STRING(),
                  0,
                  HOODIE_RECORD_KEY_COL_POS
              ),
              new ValueLiteralExpression(instantRange.getEndInstant())
          ),
          DataTypes.BOOLEAN()
      );
    } else if (instantRange.getEndInstant() == null) {
      return new CallExpression(
          FunctionIdentifier.of("greaterThan"),
          BuiltInFunctionDefinitions.GREATER_THAN,
          Arrays.asList(
              new FieldReferenceExpression(
                  HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                  DataTypes.STRING(),
                  0,
                  HOODIE_RECORD_KEY_COL_POS
              ),
              new ValueLiteralExpression(instantRange.getStartInstant())
          ),
          DataTypes.BOOLEAN()
      );
    } else {
      return new CallExpression(
          FunctionIdentifier.of("AND"),
          BuiltInFunctionDefinitions.AND,
          Arrays.asList(
              new CallExpression(
                  FunctionIdentifier.of("greaterThan"),
                  BuiltInFunctionDefinitions.GREATER_THAN,
                  Arrays.asList(
                      new FieldReferenceExpression(
                          HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                          DataTypes.STRING(),
                          0,
                          HOODIE_RECORD_KEY_COL_POS
                      ),
                      new ValueLiteralExpression(instantRange.getStartInstant())
                  ),
                  DataTypes.BOOLEAN()
              ),
              new CallExpression(
                  FunctionIdentifier.of("lessThanOrEqual"),
                  BuiltInFunctionDefinitions.LESS_THAN_OR_EQUAL,
                  Arrays.asList(
                      new FieldReferenceExpression(
                          HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                          DataTypes.STRING(),
                          0,
                          HOODIE_RECORD_KEY_COL_POS
                      ),
                      new ValueLiteralExpression(instantRange.getEndInstant())
                  ),
                  DataTypes.BOOLEAN()
              )
          ),
          DataTypes.BOOLEAN()
      );
    }
  }

  public static int getRecordKeyIndex(Schema requiredSchemaWithMeta) {
    List<Schema.Field> fields = requiredSchemaWithMeta.getFields();
    int index = 0;
    for (; index < fields.size(); index++) {
      if (fields.get(index).name().equalsIgnoreCase(HoodieRecord.RECORD_KEY_METADATA_FIELD)) {
        break;
      }
    }
    return index;
  }

  public static List<RecordReader<HoodieRecord>> createLsmRecordReaders(List<ClosableIterator<RowData>> iterators,
                                                                        int spillThreshold,
                                                                        RowDataSerializer rowDataSerializer,
                                                                        IOManager ioManager,
                                                                        int pageSize,
                                                                        int recordKeyIndex,
                                                                        int level1Index) throws IOException {
    if (ioManager == null) {
      return iterators.stream()
          .map(iter -> new FlinkRecordReader(iter, recordKeyIndex))
          .collect(Collectors.toList());
    }

    int readDirectlySize = Math.min(iterators.size(), spillThreshold);

    List<RecordReader<HoodieRecord>> readers = iterators.subList(0, readDirectlySize).stream().map(iter -> {
      return new FlinkRecordReader(iter, recordKeyIndex);
    }).collect(Collectors.toList());

    if (iterators.size() > spillThreshold) {
      int spillSize = iterators.size() - readDirectlySize;
      LOG.info("Start spilling for clustering. " + spillSize + " files will be spilled.");
      HoodieTimer timer = HoodieTimer.start();

      for (int i = spillThreshold; i < iterators.size(); i++) {
        if (level1Index == i) {
          readers.add(new FlinkRecordReader(iterators.get(i), recordKeyIndex));
        } else {
          try {
            readers.add(new FlinkExternalRecordReader(
                iterators.get(i),
                rowDataSerializer,
                recordKeyIndex,
                ioManager,
                pageSize)
            );
          } catch (Exception e) {
            LOG.error("Spill failed. Deleting all spilled files.");
            // 清理其他已完成溢写的文件
            for (RecordReader reader : readers) {
              reader.close();
            }
            throw e;
          }
        }
      }

      long spillDuration = timer.endTimer();
      LOG.info("Cost " + spillDuration + " ms to spill " + spillSize + " files to disk.");
    }

    return readers;
  }

  public static void setupMergerConfig(Configuration conf) {
    // 考虑两种情况 1.用户指定了impls；2.用户没有指定impls
    if (!conf.contains(FlinkOptions.RECORD_MERGER_IMPLS)) {
      // 如果用户没有设置Record merger，则默认给定基于commit time的 record merger
      conf.set(FlinkOptions.RECORD_MERGER_IMPLS, CommitTimeFlinkRecordMerger.class.getName());
      conf.set(FlinkOptions.RECORD_MERGER_STRATEGY, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID);
      conf.set(FlinkOptions.TABLE_RECORD_MERGER_STRATEGY, COMMIT_TIME_BASED_MERGE_STRATEGY_UUID);
    } else {
      // 如果用户指定的record merger，则1. 校验merger不能是avro merger；2. 检查用户是否设置的strategy，如果没有则自动补全
      String className = conf.get(FlinkOptions.RECORD_MERGER_IMPLS);
      ValidationUtils.checkArgument(!className.equalsIgnoreCase(HoodieAvroRecordMerger.class.getName()),
          "LSM 不支持 " + HoodieAvroRecordMerger.class.getName());
      HoodieRecordMerger merger = ReflectionUtils.loadClass(className);
      conf.set(FlinkOptions.RECORD_MERGER_STRATEGY, merger.getMergingStrategy());
      conf.set(FlinkOptions.TABLE_RECORD_MERGER_STRATEGY, merger.getMergingStrategy());
    }
  }

  public static void setupLSMConfig(Configuration conf) {
    // Step1: parquet 相关参数
    if (!conf.containsKey(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key())) {
      // 默认是用ZSTD压缩算法
      conf.setString(HoodieStorageConfig.PARQUET_COMPRESSION_CODEC_NAME.key(), "zstd");
    }

    if (!conf.containsKey(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key())) {
      // Flink 写L0层的时候 默认block size 32M，避免流读和Clustering时候的OOM问题
      // 后续的Flink 的 L1 Major Compaction的时候 重新刷为128M LSMHoodieRowDataCreateHandle
      conf.setString(HoodieStorageConfig.PARQUET_BLOCK_SIZE.key(), String.valueOf(32 * 1024 * 1024));
    }

    // Step2: 表服务相关参数
    // 强制关闭Compaction
    conf.set(FlinkOptions.COMPACTION_SCHEDULE_ENABLED, false);
    conf.set(FlinkOptions.COMPACTION_ASYNC_ENABLED, false);
    // 强制关闭普通Clustering
    conf.set(FlinkOptions.CLUSTERING_SCHEDULE_ENABLED, false);
    conf.set(FlinkOptions.CLUSTERING_ASYNC_ENABLED, false);
    if (!conf.contains(FlinkOptions.LSM_CLUSTERING_PLAN_STRATEGY_CLASS)) {
      // 设置Flink LSM Clustering plan 策略
      conf.set(FlinkOptions.LSM_CLUSTERING_PLAN_STRATEGY_CLASS, LsmBaseClusteringPlanStrategy.class.getName());
    }
    if (!conf.contains(FlinkOptions.CLEAN_RETAIN_COMMITS)) {
      // 目前clean是基于clustering构成的版本而言，而不是常规的commit数，因此这里缩减下默认值
      conf.set(FlinkOptions.CLEAN_RETAIN_COMMITS, 10);
    }

    // TODO read footer 只有在 支持流拷贝 且 event-time record merger下才会生效
    // if (conf.get(FlinkOptions.RECORD_MERGER_STRATEGY).equalsIgnoreCase(EVENT_TIME_BASED_MERGE_STRATEGY_UUID)
    //     && !conf.containsKey(HoodieClusteringConfig.LSM_CLUSTERING_READFOOTER_ENABLED.key())) {
    //   conf.set(FlinkOptions.READFOOTER_ENABLED, true);
    // }

    // Step3：写时参数
    // 多写参数默认打开
    // 默认开启Lazy
    if (!conf.containsKey(HoodieWriteConfig.MUTLIPLE_WRITE_ENABLE.key())) {
      conf.setBoolean(HoodieWriteConfig.MUTLIPLE_WRITE_ENABLE.key(), true);
      conf.setString(HoodieCleanConfig.FAILED_WRITES_CLEANER_POLICY.key(), "LAZY");
    }

    // Step4: 对于index类型的校验，LSM 目前只支持Bucket Index
    String indexType = conf.get(FlinkOptions.INDEX_TYPE);
    ValidationUtils.checkArgument(indexType.equalsIgnoreCase(HoodieIndex.IndexType.BUCKET.name()), "LSM 目前只能配合Bucket Index使用");
  }
}
