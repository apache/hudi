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

import org.apache.hudi.common.avro.VariantSchemaUtils;
import org.apache.hudi.common.config.RecordMergeMode;
import org.apache.hudi.common.engine.EngineType;
import org.apache.hudi.common.engine.HoodieReaderContext;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecordMerger;
import org.apache.hudi.common.schema.HoodieProjectionMask;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaCompatibility;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaRepair;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.schema.internal.HoodieSchemaException;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.util.HoodieRecordUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.ClosableIterator;
import org.apache.hudi.common.util.collection.CloseableMappingIterator;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.core.io.storage.HoodieIOFactory;
import org.apache.hudi.exception.HoodieAvroSchemaException;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.utils.HiveTypeUtils;
import org.apache.hudi.hadoop.utils.HoodieArrayWritableSchemaUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.avro.AvroSerdeException;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.parquet.avro.AvroSchemaConverter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.hudi.common.config.HoodieReaderConfig.MERGE_TYPE;
import static org.apache.hudi.common.config.HoodieReaderConfig.REALTIME_SKIP_MERGE;
import static org.apache.hudi.common.config.HoodieReaderConfig.RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY;
import static org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader.DEFAULT_REALTIME_SKIP_MERGE;
import static org.apache.hudi.hadoop.realtime.HoodieRealtimeRecordReader.REALTIME_SKIP_MERGE_PROP;

/**
 * {@link HoodieReaderContext} for Hive-specific {@link HoodieFileGroupReaderBasedRecordReader}.
 */
public class HiveHoodieReaderContext extends HoodieReaderContext<ArrayWritable> {
  protected final HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator;
  private RecordReader<NullWritable, ArrayWritable> firstRecordReader = null;

  private final List<String> partitionCols;
  private final Set<String> partitionColSet;

  protected HiveHoodieReaderContext(HoodieFileGroupReaderBasedRecordReader.HiveReaderCreator readerCreator,
                                    List<String> partitionCols,
                                    StorageConfiguration<?> storageConfiguration,
                                    HoodieTableConfig tableConfig) {
    super(storageConfiguration, tableConfig, Option.empty(), Option.empty(), new HiveRecordContext(tableConfig));
    if (storageConfiguration.getString(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS).isEmpty()) {
      // Overriding default treatment of repeated groups in Parquet
      storageConfiguration.set(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, "false");
    }
    this.readerCreator = readerCreator;
    this.partitionCols = partitionCols;
    this.partitionColSet = new HashSet<>(this.partitionCols);
  }

  private void setSchemas(JobConf jobConf, HoodieSchema dataSchema, HoodieSchema requiredSchema) {
    List<String> dataColumnNameList = dataSchema.getFields().stream().map(f -> f.name().toLowerCase(Locale.ROOT)).collect(Collectors.toList());
    jobConf.set(serdeConstants.LIST_COLUMNS, String.join(",", dataColumnNameList));
    List<TypeInfo> columnTypes;
    try {
      columnTypes = HiveTypeUtils.generateColumnTypes(dataSchema);
    } catch (AvroSerdeException e) {
      throw new HoodieAvroSchemaException(String.format("Failed to generate hive column types from schema: %s, due to %s", dataSchema, e));
    }
    jobConf.set(serdeConstants.LIST_COLUMN_TYPES, columnTypes.stream().map(TypeInfo::getTypeName).collect(Collectors.joining(",")));
    // don't replace `f -> f.name()` with lambda reference
    String readColNames = requiredSchema.getFields().stream().map(HoodieSchemaField::name).collect(Collectors.joining(","));
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR, readColNames);
    jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, requiredSchema.getFields()
        .stream().map(f -> String.valueOf(dataSchema.getField(f.name()).get().pos())).collect(Collectors.joining(",")));
  }

  @Override
  public ClosableIterator<ArrayWritable> getFileRecordIterator(StoragePath filePath, long start, long length, HoodieSchema dataSchema,
                                                               HoodieSchema requiredSchema, HoodieStorage storage) throws IOException {
    return getFileRecordIterator(filePath, null, start, length, dataSchema, requiredSchema, storage);
  }

  @Override
  public ClosableIterator<ArrayWritable> getFileRecordIterator(
      StoragePathInfo storagePathInfo, long start, long length, HoodieSchema dataSchema, HoodieSchema requiredSchema, HoodieStorage storage) throws IOException {
    return getFileRecordIterator(storagePathInfo.getPath(), storagePathInfo.getLocations(), start, length, dataSchema, requiredSchema, storage);
  }

  private ClosableIterator<ArrayWritable> getFileRecordIterator(StoragePath filePath, String[] hosts, long start, long length, HoodieSchema dataSchema,
                                                                HoodieSchema requiredSchema, HoodieStorage storage) throws IOException {
    if (filePath.toString().endsWith(HoodieFileFormat.LANCE.getFileExtension())) {
      throw new UnsupportedOperationException(HoodieFileFormat.LANCE_UNSUPPORTED_ERROR_MSG);
    }
    if (filePath.toString().endsWith(HoodieFileFormat.VORTEX.getFileExtension())) {
      throw new UnsupportedOperationException(HoodieFileFormat.VORTEX_SPARK_ONLY_ERROR_MSG);
    }
    // mdt file schema irregular and does not work with this logic. Also, log file evolution is handled inside the log block
    boolean isParquetOrOrc = filePath.getFileExtension().equals(HoodieFileFormat.PARQUET.getFileExtension())
        || filePath.getFileExtension().equals(HoodieFileFormat.ORC.getFileExtension());

    // Read file schema and repair logical types if needed
    HoodieSchema fileSchema;
    if (isParquetOrOrc) {
      HoodieSchema rawFileSchema = HoodieIOFactory.getIOFactory(storage).getFileFormatUtils(filePath).readSchema(storage, filePath);
      fileSchema = HoodieSchemaRepair.repairLogicalTypes(rawFileSchema, dataSchema);
    } else {
      fileSchema = dataSchema;
    }

    // Fail fast on shredded variant columns: this reader hands the file to a plain
    // parquet-avro read at the requested {metadata, value} projection, so a file whose variant
    // group carries typed_value would come back with silent nulls (the typed rows keep their
    // payload in typed_value, which the projection drops). Detection is shape-based on the
    // footer schema and anchored on the requested column being a variant, so plain user structs
    // of the same shape are left alone. toShreddedReadSchema recurses through structs, array
    // elements and map values, matching the row writer, which shreds nested variants too.
    // The flagged columns are split by Hive's read column names on the outer conf, since the
    // per-file copy below gets requiredSchema's names from setSchemas and cannot tell the two
    // apart: a column Hive selected fails as Hive-visible nulls; a column only requiredSchema
    // names is there for merging (a CUSTOM merge whose merger is not projection compatible reads
    // the whole table schema; a merger can also list it as mandatory) and fails too, because the
    // reader materializes it at {metadata, value} and the merger would consume the nulls -- unless
    // the read skips merging, in which case no merger runs at all and the merge-only bucket is
    // harmless (see isSkipMerge). Hive writes the full name list for `select *` and none for
    // count(*), whose requested schema is then empty
    // (HoodieFileGroupReaderBasedRecordReader.createRequestedSchema), so nothing is flagged unless
    // merging widens it. A read whose nested column paths
    // (hive.io.file.readNestedColumn.paths) all miss the shredded group is not flagged either:
    // Hive's parquet reader materializes only the paths it is given, and the mask rewrite below
    // already handles the compacted projection such a read comes back in.
    if (isParquetOrOrc && requiredSchema.getType() == HoodieSchemaType.RECORD) {
      HoodieSchema shreddedReadSchema = VariantSchemaUtils.toShreddedReadSchema(requiredSchema, fileSchema);
      if (shreddedReadSchema != requiredSchema) {
        List<String> shreddedPaths = new ArrayList<>();
        collectShreddedVariantPaths(requiredSchema, shreddedReadSchema, "", shreddedPaths);
        Configuration conf = storage.getConf().unwrapAs(Configuration.class);
        Set<String> requestedColumns = Arrays.stream(HoodieColumnProjectionUtils.getReadColumnNames(conf))
            .map(name -> name.trim().toLowerCase(Locale.ROOT))
            .collect(Collectors.toSet());
        List<String> shreddedColumns = HoodieColumnProjectionUtils.columnsReadingShreddedPaths(conf, shreddedPaths);
        Map<Boolean, List<String>> byHiveRequest = shreddedColumns.stream()
            .collect(Collectors.partitioningBy(requestedColumns::contains));
        List<String> hiveReads = byHiveRequest.get(true);
        List<String> mergeOnly = byHiveRequest.get(false);
        if (!hiveReads.isEmpty()) {
          throw new HoodieException(String.format(
              "Column(s) '%s' of %s hold a shredded variant (typed_value present); the Hive reader "
                  + "cannot reconstruct shredded variants. Read the table with Spark 4.1+, or "
                  + "rewrite it unshredded (e.g. cluster with "
                  + "hoodie.parquet.variant.write.shredding.enabled=false).",
              String.join(", ", hiveReads), filePath));
        }
        if (!mergeOnly.isEmpty() && !isSkipMerge(conf)) {
          Option<HoodieRecordMerger> merger = getRecordMerger();
          String mergerName = merger != null && merger.isPresent()
              ? "the record merger (" + merger.get().getClass().getName() + ")" : "the record merger";
          throw new HoodieException(String.format(
              "Column(s) '%s' of %s hold a shredded variant (typed_value present); the query does not select "
                  + "them but %s reads them for merging (the required schema is wider than the query), and the "
                  + "Hive reader cannot reconstruct shredded variants, so the merger would be handed nulls. Read "
                  + "the table with Spark 4.1+, or rewrite it unshredded (e.g. cluster with "
                  + "hoodie.parquet.variant.write.shredding.enabled=false).",
              String.join(", ", mergeOnly), filePath, mergerName));
        }
      }
    }

    // Prune the required schema based on the file schema
    HoodieSchema actualRequiredSchema = isParquetOrOrc ? HoodieSchemaUtils.pruneDataSchema(fileSchema, requiredSchema, Collections.emptySet()) : requiredSchema;

    JobConf jobConfCopy = new JobConf(storage.getConf().unwrapAs(Configuration.class));
    if (getNeedsBootstrapMerge()) {
      // Hive PPD works at row-group level and only enabled when hive.optimize.index.filter=true;
      // The above config is disabled by default. But when enabled, would cause misalignment between
      // skeleton and bootstrap file. We will disable them specifically when query needs bootstrap and skeleton
      // file to be stitched.
      // This disables row-group filtering
      jobConfCopy.unset(TableScanDesc.FILTER_EXPR_CONF_STR);
      jobConfCopy.unset(ConvertAstToSearchArg.SARG_PUSHDOWN);
    }

    // Move the partition cols to the end, because in some cases it has issues if we don't do that
    List<String> reorderedFieldNames = Stream.concat(
        fileSchema.getFields().stream()
            .map(f -> f.name().toLowerCase(Locale.ROOT))
            .filter(n -> !partitionColSet.contains(n)),
        partitionCols.stream()
            .filter(c -> fileSchema.getField(c).isPresent())
    ).collect(Collectors.toList());
    HoodieSchema modifiedDataSchema = HoodieSchemaUtils.generateProjectionSchema(fileSchema, reorderedFieldNames);

    setSchemas(jobConfCopy, modifiedDataSchema, actualRequiredSchema);
    InputSplit inputSplit = new FileSplit(new Path(filePath.toString()), start, length, hosts);
    RecordReader<NullWritable, ArrayWritable> recordReader = readerCreator.getRecordReader(inputSplit, jobConfCopy, modifiedDataSchema);
    if (firstRecordReader == null) {
      firstRecordReader = recordReader;
    }
    ClosableIterator<ArrayWritable> recordIterator = new RecordReaderValueIterator<>(recordReader);
    HoodieProjectionMask physicalMask = HoodieColumnProjectionUtils.buildNestedProjectionMask(jobConfCopy, modifiedDataSchema);
    if (physicalMask.isAll() && HoodieSchemaCompatibility.areSchemasProjectionEquivalent(modifiedDataSchema, requiredSchema)) {
      return recordIterator;
    }
    // record reader puts the required columns in the positions of the data schema and nulls the rest of the columns;
    // physicalMask additionally tells the rewrite where struct sub-fields landed when Hive's
    // Parquet reader compacted nested-column projection.
    return new CloseableMappingIterator<>(recordIterator,
        record -> HoodieArrayWritableSchemaUtils.rewriteRecordWithNewSchema(record, modifiedDataSchema, requiredSchema, Collections.emptyMap(), physicalMask));
  }

  /**
   * Whether this read skips merging, derived the way {@link HoodieFileGroupReaderBasedRecordReader}
   * derives it for the file group reader: {@code hoodie.datasource.merge.type} wins when the job
   * sets it, and the legacy {@code hoodie.realtime.merge.skip} flag only fills it in otherwise.
   *
   * <p>A skip-merge read builds an UnmergedFileGroupRecordBuffer, whose processNextDataRecord is a
   * no-op, so no merger ever reads the columns the required schema carries beyond the query.
   * generateRequiredSchema still widens to the whole table schema on a CUSTOM merge and this reader
   * still materializes the shredded variant as nulls, but nothing consumes them and the file group
   * reader's output converter projects the record back to the requested schema before
   * {@link HoodieFileGroupReaderBasedRecordReader} hands it to Hive, so the merge-only bucket must
   * not fail such a read. The Hive-visible bucket is unaffected: those columns leave the reader
   * either way.
   */
  private static boolean isSkipMerge(Configuration conf) {
    String mergeType = conf.get(MERGE_TYPE.key());
    if (mergeType != null) {
      return REALTIME_SKIP_MERGE.equalsIgnoreCase(mergeType.trim());
    }
    return Boolean.parseBoolean(conf.get(REALTIME_SKIP_MERGE_PROP, DEFAULT_REALTIME_SKIP_MERGE));
  }

  /**
   * Collects into {@code paths} the Hive-form dotted path of every variant node
   * {@link VariantSchemaUtils#toShreddedReadSchema} swapped, i.e. every requested variant the file
   * holds shredded. {@code shreddedRead} is {@code required} with exactly those nodes replaced by
   * their on-disk form, so the two are walked in parallel from {@code path}. Only record fields add
   * a segment: Hive truncates its nested column paths at a LIST or MAP column, so an array element
   * and a map value share their column's path, matching the paths the legacy guard collects.
   */
  private static void collectShreddedVariantPaths(HoodieSchema required, HoodieSchema shreddedRead,
                                                  String path, List<String> paths) {
    HoodieSchema requiredNode = required.getNonNullType();
    HoodieSchema readNode = shreddedRead.getNonNullType();
    if (requiredNode.getType() == HoodieSchemaType.VARIANT && isShreddedVariantNode(readNode)) {
      paths.add(path);
      return;
    }
    if (requiredNode.getType() != readNode.getType()) {
      return;
    }
    switch (requiredNode.getType()) {
      case RECORD:
        for (HoodieSchemaField field : requiredNode.getFields()) {
          Option<HoodieSchemaField> readField = readNode.getField(field.name());
          if (readField.isPresent()) {
            String name = field.name().toLowerCase(Locale.ROOT);
            collectShreddedVariantPaths(field.schema(), readField.get().schema(),
                path.isEmpty() ? name : path + "." + name, paths);
          }
        }
        break;
      case ARRAY:
        collectShreddedVariantPaths(requiredNode.getElementType(), readNode.getElementType(), path, paths);
        break;
      case MAP:
        collectShreddedVariantPaths(requiredNode.getValueType(), readNode.getValueType(), path, paths);
        break;
      default:
        break;
    }
  }

  /**
   * Whether this is the on-disk, typed_value-bearing form {@code toShreddedReadSchema} swaps in: a
   * variant that kept its logical type answers directly, while a footer-derived one comes back as
   * a plain record and is recognised by its {@code typed_value} member.
   */
  private static boolean isShreddedVariantNode(HoodieSchema schema) {
    if (schema.getType() == HoodieSchemaType.VARIANT) {
      return ((HoodieSchema.Variant) schema).isShredded();
    }
    return schema.getType() == HoodieSchemaType.RECORD
        && schema.getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD).isPresent();
  }

  @Override
  public Option<HoodieRecordMerger> getRecordMerger(RecordMergeMode mergeMode, String mergeStrategyId, String mergeImplClasses) {
    // TODO(HUDI-7843):
    // get rid of event time and commit time ordering. Just return Option.empty
    switch (mergeMode) {
      case EVENT_TIME_ORDERING:
        return Option.of(new DefaultHiveRecordMerger());
      case COMMIT_TIME_ORDERING:
        return Option.of(new OverwriteWithLatestHiveRecordMerger());
      case CUSTOM:
      default:
        Option<HoodieRecordMerger> recordMerger = HoodieRecordUtils.createValidRecordMerger(EngineType.JAVA, mergeImplClasses, mergeStrategyId);
        if (recordMerger.isEmpty()) {
          throw new IllegalArgumentException("No valid hive merger implementation set for `"
              + RECORD_MERGE_IMPL_CLASSES_WRITE_CONFIG_KEY + "`");
        }
        return recordMerger;
    }
  }

  @Override
  public ClosableIterator<ArrayWritable> mergeBootstrapReaders(ClosableIterator<ArrayWritable> skeletonFileIterator,
                                                               HoodieSchema skeletonRequiredSchema,
                                                               ClosableIterator<ArrayWritable> dataFileIterator,
                                                               HoodieSchema dataRequiredSchema,
                                                               List<Pair<String, Object>> partitionFieldsAndValues) {
    int skeletonLen = skeletonRequiredSchema.getFields().size();
    int dataLen = dataRequiredSchema.getFields().size();
    int[] partitionFieldPositions = partitionFieldsAndValues.stream()
        .map(pair -> dataRequiredSchema.getField(pair.getKey()).orElseThrow(() ->
            new HoodieSchemaException("Partition field '" + pair.getKey() + "' not found in data required schema")).pos())
        .mapToInt(Integer::intValue).toArray();
    Writable[] convertedPartitionValues = partitionFieldsAndValues.stream().map(Pair::getValue).toArray(Writable[]::new);
    return new ClosableIterator<ArrayWritable>() {

      private final ArrayWritable returnWritable = new ArrayWritable(Writable.class);

      @Override
      public boolean hasNext() {
        if (dataFileIterator.hasNext() != skeletonFileIterator.hasNext()) {
          throw new IllegalStateException("bootstrap data file iterator and skeleton file iterator are out of sync");
        }
        return dataFileIterator.hasNext();
      }

      @Override
      public ArrayWritable next() {
        Writable[] skeletonWritable = skeletonFileIterator.next().get();
        Writable[] dataWritable = dataFileIterator.next().get();
        for (int i = 0; i < partitionFieldPositions.length; i++) {
          if (dataWritable[partitionFieldPositions[i]] == null || dataWritable[partitionFieldPositions[i]] instanceof NullWritable) {
            dataWritable[partitionFieldPositions[i]] = convertedPartitionValues[i];
          }
        }
        Writable[] mergedWritable = new Writable[skeletonLen + dataLen];
        System.arraycopy(skeletonWritable, 0, mergedWritable, 0, skeletonLen);
        System.arraycopy(dataWritable, 0, mergedWritable, skeletonLen, dataLen);
        returnWritable.set(mergedWritable);
        return returnWritable;
      }

      @Override
      public void close() {
        skeletonFileIterator.close();
        dataFileIterator.close();
      }
    };
  }

  public long getPos() throws IOException {
    if (firstRecordReader != null) {
      return firstRecordReader.getPos();
    }
    throw new IllegalStateException("getProgress() should not be called before a record reader has been initialized");
  }

  public float getProgress() throws IOException {
    if (firstRecordReader != null) {
      return firstRecordReader.getProgress();
    }
    throw new IllegalStateException("getProgress() should not be called before a record reader has been initialized");
  }
}
