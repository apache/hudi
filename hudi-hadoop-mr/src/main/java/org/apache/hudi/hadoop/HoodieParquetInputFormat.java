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

package org.apache.hudi.hadoop;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.avro.HoodieTimestampAwareParquetInputFormat;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;
import org.apache.hudi.internal.schema.InternalSchema;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.storage.HoodieStorageUtils;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.hudi.common.util.TablePathUtils.getTablePath;
import static org.apache.hudi.common.util.TablePathUtils.isHoodieTablePath;
import static org.apache.hudi.hadoop.fs.HadoopFSUtils.convertToStoragePath;
import static org.apache.hudi.hadoop.utils.HoodieInputFormatUtils.shouldUseFilegroupReader;

/**
 * HoodieInputFormat which understands the Hoodie File Structure and filters files based on the Hoodie Mode. If paths
 * that does not correspond to a hoodie table then they are passed in as is (as what FileInputFormat.listStatus()
 * would do). The JobConf could have paths from multiple Hoodie/Non-Hoodie tables
 */
@UseRecordReaderFromInputFormat
@UseFileSplitsFromInputFormat
public class HoodieParquetInputFormat extends HoodieParquetInputFormatBase {

  private static final Logger LOG = LoggerFactory.getLogger(HoodieParquetInputFormat.class);

  private boolean supportAvroRead = false;

  public HoodieParquetInputFormat() {
    super(new HoodieCopyOnWriteTableInputFormat());
    initAvroInputFormat();
  }

  protected HoodieParquetInputFormat(HoodieCopyOnWriteTableInputFormat delegate) {
    super(delegate);
    initAvroInputFormat();
  }

  /**
   * Spark2 use `parquet.hadoopParquetInputFormat` in `com.twitter:parquet-hadoop-bundle`.
   * So that we need to distinguish the constructions of classes with
   * `parquet.hadoopParquetInputFormat` or `org.apache.parquet.hadoop.ParquetInputFormat`.
   * If we use `org.apache.parquet:parquet-hadoop`, we can use `HudiAvroParquetInputFormat`
   * in Hive or Spark3 to get timestamp with correct type.
   */
  private void initAvroInputFormat() {
    try {
      Constructor[] constructors = ParquetRecordReaderWrapper.class.getConstructors();
      if (Arrays.stream(constructors)
          .anyMatch(c -> c.getParameterCount() > 0 && c.getParameterTypes()[0]
              .getName().equals(ParquetInputFormat.class.getName()))) {
        supportAvroRead = true;
      }
    } catch (SecurityException e) {
      throw new HoodieException("Failed to check if support avro reader: " + e.getMessage(), e);
    }
  }

  private static boolean checkIfHudiTable(final InputSplit split, final JobConf job) {
    try {
      Path inputPath = ((FileSplit) split).getPath();
      FileSystem fs = inputPath.getFileSystem(job);
      HoodieStorage storage = HoodieStorageUtils.getStorage(
          HadoopFSUtils.convertToStoragePath(inputPath), HadoopFSUtils.getStorageConf(fs.getConf()));
      return getTablePath(storage, convertToStoragePath(inputPath))
          .map(path -> isHoodieTablePath(storage, path)).orElse(false);
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public RecordReader<NullWritable, ArrayWritable> getRecordReader(final InputSplit split, final JobConf job,
                                                                   final Reporter reporter) throws IOException {
    HoodieRealtimeInputFormatUtils.addProjectionField(job, job.get(hive_metastoreConstants.META_TABLE_PARTITION_COLUMNS, "").split("/"));
    if (shouldUseFilegroupReader(job, split)) {
      try {
        if (!(split instanceof FileSplit) || !checkIfHudiTable(split, job)) {
          return super.getRecordReader(split, job, reporter);
        }
        if (supportAvroRead && HoodieColumnProjectionUtils.supportTimestamp(job)) {
          return new HoodieFileGroupReaderBasedRecordReader((s, j, d) -> {
            try {
              return new ParquetRecordReaderWrapper(new HoodieTimestampAwareParquetInputFormat(Option.empty(), Option.of(d)), s, j, reporter);
            } catch (InterruptedException e) {
              throw new RuntimeException(e);
            }
          }, split, job);
        } else {
          return new HoodieFileGroupReaderBasedRecordReader((s, j, d) -> super.getRecordReader(s, j, reporter), split, job);
        }
      } catch (final IOException e) {
        throw new RuntimeException("Cannot create a RecordReaderWrapper", e);
      }
    }
    // TODO enable automatic predicate pushdown after fixing issues
    // FileSplit fileSplit = (FileSplit) split;
    // HoodieTableMetadata metadata = getTableMetadata(fileSplit.getPath().getParent());
    // String tableName = metadata.getTableName();
    // String mode = HoodieHiveUtil.readMode(job, tableName);

    // if (HoodieHiveUtil.INCREMENTAL_SCAN_MODE.equals(mode)) {
    // FilterPredicate predicate = constructHoodiePredicate(job, tableName, split);
    // LOG.info("Setting parquet predicate push down as " + predicate);
    // ParquetInputFormat.setFilterPredicate(job, predicate);
    // clearOutExistingPredicate(job);
    // }
    if (split instanceof BootstrapBaseFileSplit) {
      return createBootstrappingRecordReader(split, job, reporter);
    }

    // adapt schema evolution
    SchemaEvolutionContext schemaEvolutionContext = new SchemaEvolutionContext(split, job);
    schemaEvolutionContext.doEvolutionForParquetFormat();

    LOG.debug("EMPLOYING DEFAULT RECORD READER - {}", split);

    return getRecordReaderInternal(split, job, reporter, schemaEvolutionContext.internalSchemaOption);
  }

  private RecordReader<NullWritable, ArrayWritable> getRecordReaderInternal(InputSplit split,
                                                                            JobConf job,
                                                                            Reporter reporter) throws IOException {
    return getRecordReaderInternal(split, job, reporter, Option.empty());
  }

  private RecordReader<NullWritable, ArrayWritable> getRecordReaderInternal(InputSplit split,
                                                                            JobConf job,
                                                                            Reporter reporter,
                                                                            Option<InternalSchema> internalSchemaOption) throws IOException {
    try {
      if (supportAvroRead && HoodieColumnProjectionUtils.supportTimestamp(job)) {
        return new ParquetRecordReaderWrapper(new HoodieTimestampAwareParquetInputFormat(internalSchemaOption, Option.empty()), split, job, reporter);
      } else {
        return super.getRecordReader(split, job, reporter);
      }
    } catch (final InterruptedException | IOException e) {
      throw new RuntimeException("Cannot create a RecordReaderWrapper", e);
    }
  }

  private RecordReader<NullWritable, ArrayWritable> createBootstrappingRecordReader(InputSplit split,
                                                                                    JobConf job,
                                                                                    Reporter reporter) throws IOException {
    BootstrapBaseFileSplit eSplit = (BootstrapBaseFileSplit) split;
    String[] rawColNames = HoodieColumnProjectionUtils.getReadColumnNames(job);
    List<Integer> rawColIds = HoodieColumnProjectionUtils.getReadColumnIDs(job);
    List<Pair<Integer, String>> projectedColsWithIndex =
        IntStream.range(0, rawColIds.size()).mapToObj(idx -> Pair.of(rawColIds.get(idx), rawColNames[idx]))
            .collect(Collectors.toList());

    List<Pair<Integer, String>> hoodieColsProjected = projectedColsWithIndex.stream()
        .filter(idxWithName -> HoodieRecord.HOODIE_META_COLUMNS.contains(idxWithName.getValue()))
        .collect(Collectors.toList());
    List<Pair<Integer, String>> externalColsProjected = projectedColsWithIndex.stream()
        .filter(idxWithName -> !HoodieRecord.HOODIE_META_COLUMNS.contains(idxWithName.getValue())
            && !HoodieHiveUtils.VIRTUAL_COLUMN_NAMES.contains(idxWithName.getValue()))
        .collect(Collectors.toList());

    // This always matches hive table description
    List<Pair<String, String>> colNameWithTypes = HoodieColumnProjectionUtils.getIOColumnNameAndTypes(job);
    List<Pair<String, String>> colNamesWithTypesForExternal = colNameWithTypes.stream()
        .filter(p -> !HoodieRecord.HOODIE_META_COLUMNS.contains(p.getKey())).collect(Collectors.toList());

    LOG.info("colNameWithTypes =" + colNameWithTypes + ", Num Entries =" + colNameWithTypes.size());

    if (hoodieColsProjected.isEmpty()) {
      return getRecordReaderInternal(eSplit.getBootstrapFileSplit(), job, reporter);
    } else if (externalColsProjected.isEmpty()) {
      return getRecordReaderInternal(split, job, reporter);
    } else {
      FileSplit rightSplit = eSplit.getBootstrapFileSplit();
      // Hive PPD works at row-group level and only enabled when hive.optimize.index.filter=true;
      // The above config is disabled by default. But when enabled, would cause misalignment between
      // skeleton and bootstrap file. We will disable them specifically when query needs bootstrap and skeleton
      // file to be stitched.
      // This disables row-group filtering
      JobConf jobConfCopy = new JobConf(job);
      jobConfCopy.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
      jobConfCopy.unset(ConvertAstToSearchArg.SARG_PUSHDOWN);

      LOG.info("Generating column stitching reader for " + eSplit.getPath() + " and " + rightSplit.getPath());
      return new BootstrapColumnStichingRecordReader(getRecordReaderInternal(eSplit, jobConfCopy, reporter),
          HoodieRecord.HOODIE_META_COLUMNS.size(),
          getRecordReaderInternal(rightSplit, jobConfCopy, reporter),
          colNamesWithTypesForExternal.size(),
          true);
    }
  }
}