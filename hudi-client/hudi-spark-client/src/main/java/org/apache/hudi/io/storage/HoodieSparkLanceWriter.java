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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.engine.TaskContextSupplier;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.io.lance.HoodieBaseLanceWriter;
import org.apache.hudi.io.storage.row.HoodieBloomFilterRowWriteSupport;
import org.apache.hudi.io.storage.row.HoodieInternalRowFileWriter;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StoragePath;

import lombok.AllArgsConstructor;
import lombok.Builder;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.MetadataBuilder;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.LanceArrowUtils;
import org.apache.spark.unsafe.types.UTF8String;
import org.lance.spark.arrow.LanceArrowWriter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_SEQNO_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.COMMIT_TIME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.FILENAME_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.PARTITION_PATH_METADATA_FIELD;
import static org.apache.hudi.common.model.HoodieRecord.HoodieMetadataField.RECORD_KEY_METADATA_FIELD;
import static org.apache.hudi.common.util.ValidationUtils.checkArgument;
import static org.apache.hudi.io.storage.BlobStructLayout.DATA_IDX;
import static org.apache.hudi.io.storage.BlobStructLayout.FIELD_COUNT;
import static org.apache.hudi.io.storage.BlobStructLayout.INLINE_UTF8;
import static org.apache.hudi.io.storage.BlobStructLayout.REF_IDX;
import static org.apache.hudi.io.storage.BlobStructLayout.TYPE_IDX;

/**
 * Spark Lance file writer implementing {@link HoodieSparkFileWriter} and {@link HoodieInternalRowFileWriter}.
 *
 * This writer integrates with Hudi's storage I/O layer and supports:
 * - Hudi metadata field population
 * - Record key tracking (for bloom filters)
 * - Sequence ID generation
 * - Min/max record key tracking
 */
public class HoodieSparkLanceWriter extends HoodieBaseLanceWriter<InternalRow, UTF8String>
    implements HoodieSparkFileWriter, HoodieInternalRowFileWriter {

  private static final String DEFAULT_TIMEZONE = "UTC";
  private static final long MIN_RECORDS_FOR_SIZE_CHECK = 100L;
  private static final long MAX_RECORDS_FOR_SIZE_CHECK = 10000L;
  // Lance column metadata key that activates blob-encoded writes on a (Large)Binary column.
  // Mirrors com.lancedb.lance.spark.utils.BlobUtils.LANCE_ENCODING_BLOB_KEY (kept locally to
  // avoid leaking that internal lance-spark utility into Hudi APIs).
  private static final String LANCE_BLOB_ENCODING_KEY = "lance-encoding:blob";
  private static final String LANCE_BLOB_ENCODING_VALUE = "true";

  private final StructType sparkSchema;
  private final Schema arrowSchema;
  private final UTF8String fileName;
  private final UTF8String instantTime;
  private final boolean populateMetaFields;
  private final Function<Long, String> seqIdGenerator;
  private final long maxFileSize;
  // Top-level BLOB column ordinals and their names (parallel arrays), used by the per-row
  // descriptor-leak guard. Empty when the schema has no BLOB columns, making the guard a no-op.
  private final int[] blobFieldOrdinals;
  private final String[] blobFieldNames;
  private long recordCountForNextSizeCheck = MIN_RECORDS_FOR_SIZE_CHECK;

  /**
   * Creates a new builder for constructing {@link HoodieSparkLanceWriter} instances.
   *
   * <p>Required parameters: {@code file}, {@code sparkSchema}, {@code taskContextSupplier}, {@code storage}.
   * <p>Optional parameters with defaults:
   * <ul>
   *   <li>{@code instantTime} — defaults to {@code null}</li>
   *   <li>{@code populateMetaFields} — defaults to {@code false}</li>
   *   <li>{@code bloomFilterOpt} — defaults to {@link Option#empty()}</li>
   *   <li>{@code maxFileSize} — defaults to {@link HoodieStorageConfig#LANCE_MAX_FILE_SIZE}</li>
   *   <li>{@code allocatorSize} — defaults to {@link HoodieStorageConfig#LANCE_WRITE_ALLOCATOR_SIZE_BYTES}</li>
   *   <li>{@code flushByteWatermark} — defaults to {@link HoodieStorageConfig#LANCE_WRITE_FLUSH_BYTE_WATERMARK}</li>
   * </ul>
   */
  @Builder(builderMethodName = "builder")
  private static HoodieSparkLanceWriter create(
      StoragePath file,
      StructType sparkSchema,
      String instantTime,
      TaskContextSupplier taskContextSupplier,
      HoodieStorage storage,
      boolean populateMetaFields,
      Option<BloomFilter> bloomFilterOpt,
      long maxFileSize,
      long allocatorSize,
      long flushByteWatermark) {
    checkArgument(maxFileSize > 0, "maxFileSize must be a positive number");
    checkArgument(allocatorSize > 0, "allocatorSize must be a positive number");
    checkArgument(flushByteWatermark > 0, "flushByteWatermark must be a positive number");
    checkArgument(flushByteWatermark < allocatorSize,
        "flushByteWatermark (" + flushByteWatermark + ") must be less than allocatorSize ("
            + allocatorSize + ") so the byte-aware flush prevents reallocations from exceeding the cap");
    return new HoodieSparkLanceWriter(file, sparkSchema, instantTime,
        taskContextSupplier, storage, populateMetaFields, bloomFilterOpt, maxFileSize,
        allocatorSize, flushByteWatermark);
  }

  /**
   * Manually declared builder class to provide default values for optional parameters.
   * Lombok fills in the remaining builder methods.
   */
  public static class HoodieSparkLanceWriterBuilder {
    private Option<BloomFilter> bloomFilterOpt = Option.empty();
    private long maxFileSize = Long.parseLong(HoodieStorageConfig.LANCE_MAX_FILE_SIZE.defaultValue());
    private long allocatorSize = Long.parseLong(HoodieStorageConfig.LANCE_WRITE_ALLOCATOR_SIZE_BYTES.defaultValue());
    private long flushByteWatermark = Long.parseLong(HoodieStorageConfig.LANCE_WRITE_FLUSH_BYTE_WATERMARK.defaultValue());
  }

  private HoodieSparkLanceWriter(StoragePath file,
                                 StructType sparkSchema,
                                 String instantTime,
                                 TaskContextSupplier taskContextSupplier,
                                 HoodieStorage storage,
                                 boolean populateMetaFields,
                                 Option<BloomFilter> bloomFilterOpt,
                                 long maxFileSize,
                                 long allocatorSize,
                                 long flushByteWatermark) {
    super(file, DEFAULT_BATCH_SIZE, allocatorSize, flushByteWatermark,
        bloomFilterOpt.map(HoodieBloomFilterRowWriteSupport::new));
    this.sparkSchema = enrichSparkSchemaForLance(sparkSchema);
    StructField[] topFields = this.sparkSchema.fields();
    List<Integer> blobOrds = new ArrayList<>();
    for (int i = 0; i < topFields.length; i++) {
      if (isBlobField(topFields[i])) {
        blobOrds.add(i);
      }
    }
    this.blobFieldOrdinals = new int[blobOrds.size()];
    this.blobFieldNames = new String[blobOrds.size()];
    for (int i = 0; i < blobOrds.size(); i++) {
      int ord = blobOrds.get(i);
      this.blobFieldOrdinals[i] = ord;
      this.blobFieldNames[i] = topFields[ord].name();
    }
    Schema baseArrow = LanceArrowUtils.toArrowSchema(this.sparkSchema, DEFAULT_TIMEZONE, true);
    // Force LargeBinary + `lance-encoding:blob=true` on each BLOB's nested `data` Arrow leaf.
    // Can't be expressed Spark-side: toArrowSchema drops nested-field metadata, and tagging
    // the parent struct triggers Lance's blob-descriptor (position/size) interpretation.
    this.arrowSchema = annotateBlobDataChildren(this.sparkSchema, baseArrow);
    this.fileName = UTF8String.fromString(file.getName());
    this.instantTime = UTF8String.fromString(instantTime);
    this.populateMetaFields = populateMetaFields;
    this.maxFileSize = maxFileSize;
    this.seqIdGenerator = recordIndex -> {
      Integer partitionId = taskContextSupplier.getPartitionIdSupplier().get();
      return HoodieRecord.generateSequenceId(instantTime, partitionId, recordIndex);
    };
  }

  /**
   * Single-pass enrichment of the Spark schema for Lance writes. For each top-level field:
   *
   * <ul>
   *   <li><b>Hudi VECTOR</b> &rarr; attach {@link LanceArrowUtils#ARROW_FIXED_SIZE_LIST_SIZE_KEY()}
   *       with the vector's dimension so {@link LanceArrowUtils#toArrowSchema} emits a native
   *       Arrow {@code FixedSizeList<elem, dim>} (Lance's vector column encoding) and
   *       {@link LanceArrowWriter} selects the fixed-size-list field writer. Only FLOAT/DOUBLE
   *       element vectors are supported on Lance today (matches lance-spark's
   *       {@code VectorUtils.shouldBeFixedSizeList}); other element types fail fast.</li>
   *   <li><b>Hudi BLOB</b> &rarr; recursively widen nullability inside the BLOB subtree, since
   *       Lance rejects non-null leaves under a non-null parent struct even when the parent's
   *       row value is null (Hudi BLOB rows legitimately produce null {@code data} for
   *       OUT_OF_LINE and null {@code reference} for INLINE). The lance-encoding:blob marker
   *       and the {@code LargeBinary} type for the nested {@code data} field are applied
   *       Arrow-side in {@link #annotateBlobDataChildren}, since lance-spark's
   *       {@code toArrowSchema} doesn't propagate Spark field metadata to struct children.</li>
   *   <li>Other fields pass through unchanged.</li>
   * </ul>
   *
   * <p>Only top-level fields are inspected; Hudi BLOB and VECTOR are top-level types in the Hudi
   * schema model.
   */
  /**
   * Fail fast if the write schema contains any VARIANT-typed column. The Lance file format
   * does not currently support VARIANT (see https://lance.org/guide/data_types/#arrow-type-system);
   * without this guard the write would fail deep in the Avro-to-Arrow conversion layer with a
   * cryptic error. Walks the schema recursively so nested VARIANT fields (inside records, unions,
   * arrays, maps) are also caught.
   */
  static void validateNoVariantColumns(HoodieSchema schema) {
    checkNoVariant(schema, "");
  }

  private static void checkNoVariant(HoodieSchema schema, String path) {
    HoodieSchemaType type = schema.getType();
    if (type == HoodieSchemaType.VARIANT) {
      throw new HoodieNotSupportedException(
          "Lance base-file format does not currently support VARIANT columns "
              + "(see https://lance.org/guide/data_types/#arrow-type-system). "
              + "Found VARIANT field at '" + (path.isEmpty() ? "<root>" : path) + "'. "
              + "Use Parquet for tables with VARIANT columns.");
    }
    switch (type) {
      case RECORD:
        for (HoodieSchemaField f : schema.getFields()) {
          String childPath = path.isEmpty() ? f.name() : path + "." + f.name();
          checkNoVariant(f.schema(), childPath);
        }
        break;
      case UNION:
        for (HoodieSchema branch : schema.getTypes()) {
          checkNoVariant(branch, path);
        }
        break;
      case ARRAY:
        checkNoVariant(schema.getElementType(), path + "[]");
        break;
      case MAP:
        checkNoVariant(schema.getValueType(), path + ".<value>");
        break;
      default:
        // Primitive or BLOB / VECTOR — nothing to recurse into for VARIANT detection.
    }
  }

  private static StructType enrichSparkSchemaForLance(StructType sparkSchema) {
    Map<Integer, HoodieSchema.Vector> vectorColumns =
        VectorConversionUtils.detectVectorColumnsFromMetadata(sparkSchema);
    StructField[] fields = sparkSchema.fields();
    StructField[] newFields = new StructField[fields.length];
    for (int i = 0; i < fields.length; i++) {
      StructField field = fields[i];
      HoodieSchema.Vector vec = vectorColumns.get(i);
      if (vec != null) {
        newFields[i] = enrichVectorField(field, vec);
      } else if (isBlobField(field)) {
        newFields[i] = enrichBlobField(field);
      } else {
        newFields[i] = field;
      }
    }
    return new StructType(newFields);
  }

  private static StructField enrichVectorField(StructField field, HoodieSchema.Vector vec) {
    HoodieSchema.Vector.VectorElementType elemType = vec.getVectorElementType();
    if (elemType != HoodieSchema.Vector.VectorElementType.FLOAT
        && elemType != HoodieSchema.Vector.VectorElementType.DOUBLE) {
      throw new HoodieNotSupportedException(
          "Lance base-file format currently supports FLOAT/DOUBLE VECTOR columns only; "
              + "got element type " + elemType + " for field '" + field.name() + "'");
    }
    Metadata enriched = new MetadataBuilder()
        .withMetadata(field.metadata())
        .putLong(LanceArrowUtils.ARROW_FIXED_SIZE_LIST_SIZE_KEY(), vec.getDimension())
        .build();
    return new StructField(field.name(), field.dataType(), field.nullable(), enriched);
  }

  private static StructField enrichBlobField(StructField field) {
    return new StructField(field.name(), forceTypeNullable(field.dataType()), true, field.metadata());
  }

  private static boolean isBlobField(StructField field) {
    Metadata md = field.metadata();
    if (md == null || !md.contains(HoodieSchema.TYPE_METADATA_FIELD)) {
      return false;
    }
    return HoodieSchema.parseTypeDescriptor(md.getString(HoodieSchema.TYPE_METADATA_FIELD))
        .getType() == HoodieSchemaType.BLOB;
  }

  private static DataType forceTypeNullable(DataType dt) {
    if (dt instanceof StructType) {
      StructField[] in = ((StructType) dt).fields();
      StructField[] out = new StructField[in.length];
      for (int i = 0; i < in.length; i++) {
        StructField f = in[i];
        out[i] = new StructField(f.name(), forceTypeNullable(f.dataType()), true, f.metadata());
      }
      return new StructType(out);
    } else if (dt instanceof ArrayType) {
      ArrayType a = (ArrayType) dt;
      return new ArrayType(forceTypeNullable(a.elementType()), true);
    } else if (dt instanceof MapType) {
      MapType m = (MapType) dt;
      return new MapType(forceTypeNullable(m.keyType()), forceTypeNullable(m.valueType()), true);
    }
    return dt;
  }

  private static Schema annotateBlobDataChildren(StructType sparkSchema, Schema arrowSchema) {
    List<Field> arrowTop = arrowSchema.getFields();
    StructField[] sparkTop = sparkSchema.fields();
    if (sparkTop.length != arrowTop.size()) {
      throw new IllegalStateException(String.format(
          "Spark/Arrow top-level field count mismatch: %d vs %d", sparkTop.length, arrowTop.size()));
    }
    List<Field> rebuilt = new ArrayList<>(arrowTop.size());
    for (int i = 0; i < sparkTop.length; i++) {
      Field af = arrowTop.get(i);
      rebuilt.add(isBlobField(sparkTop[i]) ? rewriteBlobDataChild(af) : af);
    }
    return new Schema(rebuilt);
  }

  private static Field rewriteBlobDataChild(Field blobStructField) {
    List<Field> children = blobStructField.getChildren();
    List<Field> rebuilt = new ArrayList<>(children.size());
    for (Field child : children) {
      if (HoodieSchema.Blob.INLINE_DATA_FIELD.equals(child.getName())) {
        Map<String, String> meta = new HashMap<>();
        meta.put(LANCE_BLOB_ENCODING_KEY, LANCE_BLOB_ENCODING_VALUE);
        FieldType newFt = new FieldType(true, ArrowType.LargeBinary.INSTANCE, null, meta);
        rebuilt.add(new Field(child.getName(), newFt, Collections.emptyList()));
      } else {
        rebuilt.add(child);
      }
    }
    return new Field(blobStructField.getName(), blobStructField.getFieldType(), rebuilt);
  }

  @Override
  public void writeRowWithMetadata(HoodieKey key, InternalRow row) throws IOException {
    UTF8String recordKey = UTF8String.fromString(key.getRecordKey());
    bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport -> bloomFilterWriteSupport.addKey(recordKey));
    if (populateMetaFields) {
      updateRecordMetadata(row, recordKey, key.getPartitionPath(), getWrittenRecordCount());
    }
    super.write(row);
  }

  @Override
  public void writeRow(String recordKey, InternalRow row) throws IOException {
    bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport ->
        bloomFilterWriteSupport.addKey(UTF8String.fromString(recordKey)));
    super.write(row);
  }
  
  @Override
  public void writeRow(UTF8String key, InternalRow row) throws IOException {
    bloomFilterWriteSupportOpt.ifPresent(bloomFilterWriteSupport -> bloomFilterWriteSupport.addKey(key));
    super.write(row);
  }
  
  @Override
  public void writeRow(InternalRow row) throws IOException {
    super.write(row);
  }

  @Override
  protected ArrowWriter<InternalRow> createArrowWriter(VectorSchemaRoot root) {
    return SparkArrowWriter.of(LanceArrowWriter.create(root, sparkSchema), blobFieldOrdinals, blobFieldNames);
  }

  /**
   * Check if writer can accept more records based on estimated data size.
   * Data size is approximated by accumulating Arrow buffer sizes across flushed batches,
   * analogous to {@code ParquetWriter.getDataSize()}.
   * The check is performed periodically (not on every record) and the interval adapts
   * based on the observed average record size.
   *
   * @return true if writer can accept more records, false if file size limit is reached
   */
  public boolean canWrite() {
    long writtenCount = getWrittenRecordCount();
    if (writtenCount >= recordCountForNextSizeCheck) {
      long dataSize = getDataSize();
      // In extreme cases (e.g. all records same value, high compression ratio),
      // dataSize may be 0; force avgRecordSize to at least 1 to avoid division by zero.
      long avgRecordSize = Math.max(dataSize / writtenCount, 1);
      // Return false when within ~2 records of the limit
      if (dataSize > (maxFileSize - avgRecordSize * 2)) {
        return false;
      }
      recordCountForNextSizeCheck = writtenCount + Math.min(
          // Check at halfway between current position and the limit
          Math.max(MIN_RECORDS_FOR_SIZE_CHECK, (maxFileSize / avgRecordSize - writtenCount) / 2),
          MAX_RECORDS_FOR_SIZE_CHECK);
    }
    return true;
  }

  @Override
  protected Schema getArrowSchema() {
    return arrowSchema;
  }

  /**
   * Emit Hudi's {@code hoodie.vector.columns} footer entry alongside any
   * bloom-filter metadata. Mirrors the Parquet writer (see
   * {@code HoodieRowParquetWriteSupport#init}) so Lance files carry the same
   * self-describing VECTOR descriptor list that Parquet files do.
   *
   * <p>The read side today derives VECTOR identity from the Arrow
   * {@code FixedSizeList<Float/Double, N>} type — this footer entry is a
   * forward-compat guard: it lets future readers recover the exact descriptor
   * (including fields the Arrow type cannot express, e.g. quantization tags)
   * without a writer bump.
   */
  @Override
  protected Map<String, String> additionalSchemaMetadata() {
    String value = VectorConversionUtils.buildVectorColumnsFooterValue(sparkSchema);
    if (value.isEmpty()) {
      return Collections.emptyMap();
    }
    return Collections.singletonMap(HoodieSchema.VECTOR_COLUMNS_METADATA_KEY, value);
  }

  /**
   * Update Hudi metadata fields in the InternalRow.
   *
   * @param row InternalRow to update
   * @param recordKey Record key
   * @param partitionPath Partition path
   * @param recordCount Current record count for sequence ID generation
   */
  protected void updateRecordMetadata(InternalRow row,
                                      UTF8String recordKey,
                                      String partitionPath,
                                      long recordCount) {
    row.update(COMMIT_TIME_METADATA_FIELD.ordinal(), instantTime);
    row.update(COMMIT_SEQNO_METADATA_FIELD.ordinal(), UTF8String.fromString(seqIdGenerator.apply(recordCount)));
    row.update(RECORD_KEY_METADATA_FIELD.ordinal(), recordKey);
    row.update(PARTITION_PATH_METADATA_FIELD.ordinal(), UTF8String.fromString(partitionPath));
    row.update(FILENAME_METADATA_FIELD.ordinal(), fileName);
  }

  @AllArgsConstructor(staticName = "of")
  private static class SparkArrowWriter implements ArrowWriter<InternalRow> {
    private final LanceArrowWriter lanceArrowWriter;
    // Parallel arrays of top-level BLOB column ordinals and names for the per-row guard.
    private final int[] blobFieldOrdinals;
    private final String[] blobFieldNames;

    @Override
    public void write(InternalRow row) {
      validateBlobRow(row);
      lanceArrowWriter.write(row);
    }

    /**
     * Reject descriptor-shaped BLOB rows before they are persisted. A row is descriptor-shaped when
     * a top-level BLOB struct is non-null, its type is INLINE, its data is null, and its reference
     * is non-null; this only arises when a DESCRIPTOR-mode read leaks onto a write path, and writing
     * it would silently drop the inline bytes. The legitimate empty-inline shape {INLINE, null, null}
     * stays writable.
     */
    private void validateBlobRow(InternalRow row) {
      for (int i = 0; i < blobFieldOrdinals.length; i++) {
        int ordinal = blobFieldOrdinals[i];
        if (row.isNullAt(ordinal)) {
          continue;
        }
        InternalRow blob = row.getStruct(ordinal, FIELD_COUNT);
        if (blob.isNullAt(TYPE_IDX) || !INLINE_UTF8.equals(blob.getUTF8String(TYPE_IDX))) {
          continue;
        }
        if (blob.isNullAt(DATA_IDX) && !blob.isNullAt(REF_IDX)) {
          throw new HoodieException(
              "BLOB column '" + blobFieldNames[i] + "' has an INLINE row with null data but a "
                  + "populated reference: a DESCRIPTOR-mode read leaked into the write path, and "
                  + "persisting it would silently drop the blob bytes. Internal rewrites "
                  + "(compaction, clustering, merge) and any query whose rows are written back "
                  + "must read with hoodie.read.blob.inline.mode=CONTENT.");
        }
      }
    }

    @Override
    public void reset() {
      lanceArrowWriter.reset();
    }

    @Override
    public void finishBatch() {
      lanceArrowWriter.finish();
    }
  }
}
