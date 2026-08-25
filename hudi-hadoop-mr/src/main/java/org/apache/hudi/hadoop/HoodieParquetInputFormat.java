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

import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.internal.InternalSchema;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ParquetUtils;
import org.apache.hudi.common.util.VisibleForTesting;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.hadoop.avro.HoodieTimestampAwareParquetInputFormat;
import org.apache.hudi.hadoop.fs.HadoopFSUtils;
import org.apache.hudi.hadoop.utils.HoodieHiveUtils;
import org.apache.hudi.hadoop.utils.HoodieRealtimeInputFormatUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.ql.io.parquet.read.ParquetRecordReaderWrapper;
import org.apache.hadoop.hive.ql.io.sarg.ConvertAstToSearchArg;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.parquet.hadoop.ParquetInputFormat;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.OriginalType;
import org.apache.parquet.schema.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
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
  // Compiled once: the raw columns.types screen below runs on every legacy-path split.
  private static final Pattern WHITESPACE = Pattern.compile("\\s");
  // A member a synced variant's Hive type carries, as it appears in a struct<...> type string:
  // the cheap screen that decides whether the type string is worth parsing at all.
  private static final String HIVE_VARIANT_METADATA = HoodieSchema.Variant.VARIANT_METADATA_FIELD + ":binary";

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
    // From here on the split is read by Hive's plain parquet reader, which the file-group-reader
    // guard in HiveHoodieReaderContext never sees; repeat the shredded-variant fail-fast for it.
    // A bootstrap split's own file is the skeleton, which holds meta columns only, so only its
    // bootstrap half is worth a footer read.
    if (split instanceof BootstrapBaseFileSplit) {
      validateNoShreddedVariantRead(((BootstrapBaseFileSplit) split).getBootstrapFileSplit(), job);
      return createBootstrappingRecordReader(split, job, reporter);
    }
    validateNoShreddedVariantRead(split, job);

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

    LOG.info("colNameWithTypes ={}, Num Entries ={}", colNameWithTypes, colNameWithTypes.size());

    Option<FileSplit> singleSplit = resolveSingleFileSplit(eSplit, !hoodieColsProjected.isEmpty(),
        !externalColsProjected.isEmpty());
    if (singleSplit.isPresent()) {
      return getRecordReaderInternal(singleSplit.get(), job, reporter);
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

      LOG.info("Generating column stitching reader for {} and {}", eSplit.getPath(), rightSplit.getPath());
      return new BootstrapColumnStichingRecordReader(getRecordReaderInternal(eSplit, jobConfCopy, reporter),
          HoodieRecord.HOODIE_META_COLUMNS.size(),
          getRecordReaderInternal(rightSplit, jobConfCopy, reporter),
          colNamesWithTypesForExternal.size(),
          true);
    }
  }

  /**
   * The single file backing this read, or empty when both files are needed and have to be stitched.
   *
   * <p>A bootstrap split carries two paths: the split itself is the skeleton file, which lives inside the
   * table root, and {@code getBootstrapFileSplit()} is the external source file, which does not.
   *
   * <p>The two "only one file is needed" cases both apply when a query projects no columns at all, as
   * {@code SELECT COUNT(*)} does, so the order they are tested in decides which file is read. Prefer the
   * skeleton: it is inside the table root, and bootstrap keeps a one-to-one row correspondence with the
   * external file, so a count over it is identical. Handing Hive a path outside the table root breaks its
   * vectorized reader, which derives partition values by looking the split path up in
   * {@code pathToPartitionInfo} (HUDI-5526).
   *
   * @param split                  the bootstrap split.
   * @param anyHoodieColProjected  whether the query projects any Hudi meta column.
   * @param anyExternalColProjected whether the query projects any column from the external file.
   */
  @VisibleForTesting
  static Option<FileSplit> resolveSingleFileSplit(BootstrapBaseFileSplit split,
                                                  boolean anyHoodieColProjected,
                                                  boolean anyExternalColProjected) {
    if (!anyExternalColProjected) {
      return Option.of(split);
    } else if (!anyHoodieColProjected) {
      return Option.of(split.getBootstrapFileSplit());
    } else {
      return Option.empty();
    }
  }

  /**
   * The file-group-reader path fails fast on shredded variant reads inside
   * HiveHoodieReaderContext, but a split can bypass it three ways (see
   * HoodieInputFormatUtils.shouldUseFilegroupReader): the file group reader disabled,
   * schema-on-read enabled, and bootstrap splits. Those land on Hive's plain parquet reader at
   * the synced {metadata, value} projection, which silently nulls typed_value - so repeat the
   * fail-fast for them. Only reads that request a column holding a shredded variant fail;
   * count(*) and projections that skip the variant keep working. The footer read is gated on a
   * requested column whose synced Hive type embeds the variant {metadata, value} shape, so
   * non-variant tables never pay it: the raw columns.types string is screened for the shape's
   * marker before it is parsed, keeping the type parse itself off every other table's splits.
   *
   * <p>The footer's MessageType is inspected directly, without converting it to Avro:
   * AvroSchemaConverterWithTimestampNTZ.convertINT96 throws unless parquet.avro.readInt96AsFixed
   * is set (nothing in Hudi sets it), and Spark writes timestamps as INT96 by default, so the
   * conversion would fail the very reads this guard is careful to leave working. The footer
   * carries no variant logical type either way, so its groups are matched by shape at any depth,
   * and only at a path where the requested column's parsed Hive type declares a variant node.
   *
   * <p>Hive's read column names are top-level only, so a requested struct column does not imply
   * its whole interior: nested column pruning is carried separately as dotted paths
   * (hive.io.file.readNestedColumn.paths), which
   * {@link HoodieColumnProjectionUtils#columnsReadingShreddedPaths} applies for both this guard
   * and its file-group-reader sibling in HiveHoodieReaderContext.
   *
   * <p>The guard is best-effort throughout: a malformed columns/columns.types pairing, an
   * unparseable type string, or a projection that names no column all fall through to the plain
   * parquet reader rather than failing a read it would have served.
   */
  @VisibleForTesting
  static void validateNoShreddedVariantRead(InputSplit split, JobConf job) {
    if (!(split instanceof FileSplit)) {
      return;
    }
    Path filePath = ((FileSplit) split).getPath();
    if (!filePath.getName().endsWith(HoodieFileFormat.PARQUET.getFileExtension())) {
      return;
    }
    // Screen the raw type string before anything parses it: the TypeInfoUtils parse below is a
    // cost every legacy-path split of every table would otherwise pay. Only the variant shape's
    // marker earns the parse; the exact anchor check on the parsed types is below.
    String rawIoColumnTypes = WHITESPACE.matcher(job.get(IOConstants.COLUMNS_TYPES, ""))
        .replaceAll("")
        .toLowerCase(Locale.ROOT);
    if (!rawIoColumnTypes.contains(HIVE_VARIANT_METADATA)) {
      return;
    }
    Set<String> requestedColumns = Arrays.stream(HoodieColumnProjectionUtils.getReadColumnNames(job))
        .map(name -> name.toLowerCase(Locale.ROOT))
        .collect(Collectors.toSet());
    if (requestedColumns.isEmpty()) {
      // Hive writes the FULL column-name list for `select *`: HiveInputFormat.pushProjection
      // fills in every table column with read.all.columns=false. setReadAllColumns is only
      // called by ProjectionPusher, on the JobConf it clones downstream of getRecordReader, so
      // that flag never reaches the conf seen here (verified in hive-exec 2.3.10, 3.1.3, 4.0.1).
      // Empty names here therefore means a read that materializes no column (count(*)) or a
      // caller that never projected; read.all.columns, true when untouched, is not a signal.
      return;
    }
    List<String> ioColumns = HoodieColumnProjectionUtils.getIOColumns(job);
    List<TypeInfo> ioColumnTypes;
    try {
      ioColumnTypes = TypeInfoUtils.getTypeInfosFromTypeString(job.get(IOConstants.COLUMNS_TYPES, ""));
    } catch (RuntimeException e) {
      // The screen above strips whitespace and lower-cases; the TypeInfoUtils parse tolerates
      // neither, so a string it lets through can still fail to parse. Bail out like the pairing
      // check below rather than failing a read the plain parquet reader would serve.
      LOG.debug("Skipping the shredded variant guard for {}: {} did not parse", filePath, IOConstants.COLUMNS_TYPES, e);
      return;
    }
    if (ioColumns.size() != ioColumnTypes.size()) {
      // The guard is best-effort: a malformed columns/columns.types pairing must not fail
      // reads the plain parquet reader would otherwise serve.
      return;
    }
    // The anchor, per requested column: the Hive-form paths of every node whose synced type is the
    // exact node shape struct<metadata:binary,value:binary> that HiveSchemaUtil.convertField emits
    // for a VARIANT (HMS and Glue sync both pass doFormat=false, so no spaces and no backticks).
    // A struct carrying any further member is a plain user struct that happens to hold those two,
    // and is exempt here as it is in the sibling Spark guards. One route fails open:
    // TableSchemaResolver's footer fallback (see the comment near TableSchemaResolver:118) strips
    // shredding by shape at the top level only, so a variant shredded below the top level can
    // reach the metastore with typed_value still in its synced type; that three-member struct
    // reads as a user struct here, i.e. pre-PR behaviour.
    Map<String, List<String>> variantPathsByColumn = new HashMap<>();
    for (int i = 0; i < ioColumns.size(); i++) {
      String columnName = ioColumns.get(i).toLowerCase(Locale.ROOT);
      if (!requestedColumns.contains(columnName)) {
        continue;
      }
      List<String> variantPaths = new ArrayList<>();
      collectHiveVariantPaths(ioColumnTypes.get(i), columnName, variantPaths);
      if (!variantPaths.isEmpty()) {
        variantPathsByColumn.put(columnName, variantPaths);
      }
    }
    if (variantPathsByColumn.isEmpty()) {
      return;
    }
    StoragePath storagePath = convertToStoragePath(filePath);
    HoodieStorage storage = HoodieStorageUtils.getStorage(storagePath, HadoopFSUtils.getStorageConf(job));
    MessageType fileSchema = new ParquetUtils().readMessageType(storage, storagePath);
    // The shredded groups the file holds at a path where the column's Hive type declares a variant:
    // the two sides are matched, so neither a file group of that shape under a user struct nor a
    // synced variant the file does not actually shred can flag the column on its own.
    List<String> shreddedPaths = new ArrayList<>();
    for (Type field : fileSchema.getFields()) {
      String columnName = field.getName().toLowerCase(Locale.ROOT);
      List<String> variantPaths = variantPathsByColumn.get(columnName);
      if (variantPaths == null) {
        continue;
      }
      List<String> filePaths = new ArrayList<>();
      collectShreddedVariantPaths(field, columnName, filePaths);
      filePaths.stream().filter(variantPaths::contains).forEach(shreddedPaths::add);
    }
    List<String> offendingColumns = HoodieColumnProjectionUtils.columnsReadingShreddedPaths(job, shreddedPaths);
    if (!offendingColumns.isEmpty()) {
      throw new HoodieException(String.format(
          "Column(s) '%s' of %s hold a shredded variant (typed_value present); the Hive reader "
              + "cannot reconstruct shredded variants. Read the table with Spark 4.1+, or "
              + "rewrite it unshredded (e.g. cluster with "
              + "hoodie.parquet.variant.write.shredding.enabled=false).",
          String.join(", ", offendingColumns), filePath));
    }
  }

  /**
   * Collects into {@code variantPaths} the Hive-form dotted path of every node at or beneath
   * {@code type} whose Hive type is the synced variant shape (see {@link #isVariantShapedStruct}),
   * starting at {@code path}. Only struct members add a segment: Hive truncates its nested column
   * paths at a LIST or MAP column, so a list element and a map value share their column's path.
   */
  private static void collectHiveVariantPaths(TypeInfo type, String path, List<String> variantPaths) {
    switch (type.getCategory()) {
      case STRUCT: {
        StructTypeInfo struct = (StructTypeInfo) type;
        if (isVariantShapedStruct(struct)) {
          variantPaths.add(path);
          return;
        }
        List<String> memberNames = struct.getAllStructFieldNames();
        List<TypeInfo> memberTypes = struct.getAllStructFieldTypeInfos();
        for (int i = 0; i < memberNames.size(); i++) {
          collectHiveVariantPaths(memberTypes.get(i), path + "." + memberNames.get(i).toLowerCase(Locale.ROOT), variantPaths);
        }
        break;
      }
      case LIST:
        collectHiveVariantPaths(((ListTypeInfo) type).getListElementTypeInfo(), path, variantPaths);
        break;
      case MAP:
        collectHiveVariantPaths(((MapTypeInfo) type).getMapValueTypeInfo(), path, variantPaths);
        break;
      default:
        break;
    }
  }

  /**
   * Whether {@code struct} is the Hive type a synced VARIANT gets: exactly the two binary members
   * {@code metadata} and {@code value}. A struct with a third member is a user struct - including
   * one whose third member is named typed_value, which only the footer fallback of
   * TableSchemaResolver can produce for a nested shredded variant.
   */
  private static boolean isVariantShapedStruct(StructTypeInfo struct) {
    List<String> memberNames = struct.getAllStructFieldNames();
    if (memberNames.size() != 2) {
      return false;
    }
    List<String> lowered = memberNames.stream().map(name -> name.toLowerCase(Locale.ROOT)).collect(Collectors.toList());
    return lowered.contains(HoodieSchema.Variant.VARIANT_METADATA_FIELD)
        && lowered.contains(HoodieSchema.Variant.VARIANT_VALUE_FIELD)
        && struct.getAllStructFieldTypeInfos().stream().allMatch(HoodieParquetInputFormat::isBinary);
  }

  /** Whether {@code type} is the Hive {@code binary} primitive. */
  private static boolean isBinary(TypeInfo type) {
    return type instanceof PrimitiveTypeInfo && serdeConstants.BINARY_TYPE_NAME.equals(type.getTypeName());
  }

  /**
   * Collects into {@code shreddedPaths} the Hive-form dotted path of every shredded variant group
   * at or beneath {@code type}: a group carrying both {@code typed_value} and {@code metadata}.
   * The shape is checked at every group before descending, so a shredded element is recorded at
   * its collection column's own path on either list layout. The walk stops at the first shredded
   * group on a branch - everything below it belongs to that variant.
   *
   * <p>Paths are lower-cased parquet field names joined by "." starting at {@code path}, minus the
   * levels a Hive dotted path never names, because Hive truncates its nested column paths at a
   * LIST or MAP column: the collection's repeated level, the synthetic level between a LIST and
   * its element, and a map entry's key and value. Only struct members append a segment. Which
   * levels those are is decided structurally, by parquet's own backward-compatibility rule rather
   * than by level names, so a struct element's member that happens to be called {@code element},
   * {@code key} or {@code value} keeps its segment: under a LIST group with a single child, that
   * child is the synthetic level only when it is a group with exactly one non-repeated field whose
   * name is neither {@code array} nor a {@code _tuple} suffix (the 3-level layout), and otherwise
   * it is the element itself (the 2-level layout); under a MAP group the single child is always
   * the entry level, whose key and value both carry the map's own path.
   *
   * <p>LIST and MAP are read off OriginalType rather than the LogicalTypeAnnotation that replaced
   * it: parquet 1.11 and later derive one from the other, while this module loads inside Hive,
   * whose bundled parquet can predate the annotation class entirely (the reason ParquetAdapter
   * picks its implementation reflectively). A collection group shaped unlike its annotation (no
   * single child) is walked as a plain struct, the best-effort reading.
   */
  private static void collectShreddedVariantPaths(Type type, String path, List<String> shreddedPaths) {
    if (type.isPrimitive()) {
      return;
    }
    GroupType group = type.asGroupType();
    if (group.containsField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD)
        && group.containsField(HoodieSchema.Variant.VARIANT_METADATA_FIELD)) {
      shreddedPaths.add(path);
      return;
    }
    OriginalType originalType = group.getOriginalType();
    if (originalType == OriginalType.LIST && group.getFieldCount() == 1) {
      Type repeated = group.getType(0);
      Type element = isSyntheticListLevel(repeated) ? repeated.asGroupType().getType(0) : repeated;
      collectShreddedVariantPaths(element, path, shreddedPaths);
      return;
    }
    if (originalType == OriginalType.MAP && group.getFieldCount() == 1 && !group.getType(0).isPrimitive()) {
      for (Type entryMember : group.getType(0).asGroupType().getFields()) {
        collectShreddedVariantPaths(entryMember, path, shreddedPaths);
      }
      return;
    }
    for (Type field : group.getFields()) {
      collectShreddedVariantPaths(field, path + "." + field.getName().toLowerCase(Locale.ROOT), shreddedPaths);
    }
  }

  /**
   * Whether {@code repeated}, the single child of a LIST group, is the 3-level layout's synthetic
   * level rather than the element itself. This is parquet's backward-compatibility rule (the one
   * AvroSchemaConverter applies as isElementType, negated): a repeated level that is a group with
   * exactly one non-repeated field and a name that is not one of the legacy element names is the
   * synthetic level; anything else is a 2-level layout's element, whose members are user fields.
   */
  private static boolean isSyntheticListLevel(Type repeated) {
    if (repeated.isPrimitive()) {
      return false;
    }
    GroupType group = repeated.asGroupType();
    String name = repeated.getName().toLowerCase(Locale.ROOT);
    return group.getFieldCount() == 1
        && !group.getType(0).isRepetition(Type.Repetition.REPEATED)
        && !"array".equals(name)
        && !name.endsWith("_tuple");
  }
}
