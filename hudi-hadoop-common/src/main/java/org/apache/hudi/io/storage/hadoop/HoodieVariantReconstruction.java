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

package org.apache.hudi.io.storage.hadoop;

import org.apache.hudi.common.avro.VariantSchemaUtils;
import org.apache.hudi.common.avro.VariantShreddingProvider;
import org.apache.hudi.common.avro.VariantShreddingRuntime;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.storage.HoodieStorage;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reconstructs unshredded variants when reading an already-shredded base file on the Avro
 * ({@code HoodieRecordType.AVRO}) read path.
 *
 * <p>parquet-avro does not understand variant shredding, so a shredded variant column comes back as
 * a raw {@code {metadata, value, typed_value}} record. This reads such columns at their shredded
 * (typed_value-bearing) schema, then rebuilds the standard unshredded {@code {metadata, value}}
 * variant via {@link VariantShreddingProvider#prepareRebuild} before records reach the
 * merger/writer. The Spark/InternalRow read path reconstructs natively and does not use this.
 *
 * <p>See https://github.com/apache/hudi/issues/18931.
 */
final class HoodieVariantReconstruction {

  private final HoodieSchema intermediateSchema;
  private final Rebuilder rootRebuilder;

  private HoodieVariantReconstruction(HoodieSchema intermediateSchema, Rebuilder rootRebuilder) {
    this.intermediateSchema = intermediateSchema;
    this.rootRebuilder = rootRebuilder;
  }

  /**
   * Schema to read the parquet file with: the requested schema, but with shredded variant columns
   * swapped to their file (typed_value-bearing) form so parquet-avro materializes {@code typed_value}.
   */
  HoodieSchema intermediateSchema() {
    return intermediateSchema;
  }

  /**
   * Builds a reconstruction for the given file and requested schemas, or returns {@code null} when
   * none is needed (the file has no shredded variant columns). Throws when the file has shredded
   * variant columns to reconstruct but reading shredded variants is disabled, or no provider is
   * available: either way, reading at the unshredded schema would silently drop the typed_value payload.
   * Also throws when a shredded variant column uses a typed_value encoding that cannot be read, so the
   * failure happens when the file is opened rather than on the first affected record.
   *
   * @param fileSchema        the file schema, converted from {@code fileParquetSchema}
   * @param fileParquetSchema the file's parquet schema, which carries the typed_value encodings the
   *                          Avro conversion loses
   * @param requestedSchema   the schema the records are requested at
   * @param storage           the storage the file is read with
   */
  static HoodieVariantReconstruction create(HoodieSchema fileSchema, MessageType fileParquetSchema,
                                            HoodieSchema requestedSchema, HoodieStorage storage) {
    if (requestedSchema.getType() != HoodieSchemaType.RECORD || fileSchema.getType() != HoodieSchemaType.RECORD) {
      return null;
    }

    // Records leave this reader unshredded; output field order matches the requested/intermediate order.
    HoodieSchema outputSchema = VariantSchemaUtils.stripVariantShredding(requestedSchema);
    // The list layout rule must match the one the file schema was converted with (see
    // HoodieAvroParquetReader.tryOverrideDefaultConfigs).
    Planner planner = new Planner(storage.getConf().getBoolean(AvroSchemaConverter.ADD_LIST_ELEMENT_RECORDS, true));
    Rebuilder rootRebuilder = planner.plan(outputSchema, fileSchema, fileParquetSchema, null);
    if (rootRebuilder == null) {
      // No shredded variant columns in the file: nothing to reconstruct, regardless of the flag.
      return null;
    }

    if (!storage.getConf().getBoolean(HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.key(),
        HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.defaultValue())) {
      // Reading at the unshredded schema would drop typed_value and silently corrupt variants whose
      // payload lives there, so fail fast. Mirrors the no-provider branch and Spark's
      // allowReadingShredded=false, which rejects shredded reads rather than discarding data.
      throw new HoodieException("Base file has shredded variant column(s) but reading shredded variants is "
          + "disabled (" + HoodieStorageConfig.PARQUET_VARIANT_ALLOW_READING_SHREDDED.key()
          + "=false). Enable it to reconstruct them; otherwise the typed_value payload would be silently dropped.");
    }

    VariantShreddingProvider provider = loadProvider(storage);
    if (provider == null) {
      // Reading would drop typed_value and silently corrupt variants whose payload lives there, so fail fast.
      throw new HoodieException("Base file has shredded variant column(s) and reading shredded variants is "
          + "enabled, but no VariantShreddingProvider is available to reconstruct them. Set "
          + HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key()
          + " or add a provider implementation (e.g. the Spark variant module) to the classpath.");
    }
    for (VariantColumnRebuilder variantColumn : planner.variantColumns) {
      variantColumn.prepare(provider);
    }

    return new HoodieVariantReconstruction(
        VariantSchemaUtils.toShreddedReadSchema(requestedSchema, fileSchema), rootRebuilder);
  }

  /**
   * Rebuilds shredded variant columns of {@code in} (read in the intermediate shredded shape) into
   * a record conforming to the unshredded output schema.
   */
  IndexedRecord reconstruct(IndexedRecord in) {
    return (IndexedRecord) rootRebuilder.rebuild(in);
  }

  /** Plans the rebuild of a file, collecting its shredded variant columns for preparation. */
  private static final class Planner {
    // Mirrors the file schema conversion's ADD_LIST_ELEMENT_RECORDS, which decides the list element.
    private final boolean assumeRepeatedIsListElement;
    private final List<VariantColumnRebuilder> variantColumns = new ArrayList<>();

    private Planner(boolean assumeRepeatedIsListElement) {
      this.assumeRepeatedIsListElement = assumeRepeatedIsListElement;
    }

    /**
     * Plans the rebuild for one schema position, or returns {@code null} when nothing below it is a
     * shredded variant and the value can be passed through untouched. The walk is driven by the
     * output (requested) side because that is the shape the record read from parquet has; the file
     * side, and the parquet type it was converted from, are matched into it by field name. Detection
     * is anchored by the requested side because the file schema usually comes from converting the
     * parquet footer MessageType, which loses the variant logical type; see
     * VariantSchemaUtils.isShreddedVariantTarget (#19567). Records, array elements and map values are
     * all descended into, matching what the writers can emit; see
     * {@code VariantSchemaUtils.swapShreddedVariantFields} for what actually produces a nested
     * shredded file today (both the row and the AVRO write path shred variant record members at any
     * depth off the forced-shredding property; a variant that is directly an array element or a map
     * value shreds on neither path unless the write schema itself declares typed_value there).
     */
    private Rebuilder plan(HoodieSchema outputSchema, HoodieSchema fileSchema, Type parquetType, String path) {
      if (VariantSchemaUtils.isShreddedVariantTarget(fileSchema, outputSchema)) {
        if (parquetType == null || parquetType.isPrimitive()) {
          throw new IllegalStateException("Shredded variant column '" + path
              + "' has no matching parquet group in the file schema: " + parquetType);
        }
        VariantColumnRebuilder variantColumn = new VariantColumnRebuilder(
            path, parquetType.asGroupType(), outputSchema.getNonNullType().getAvroSchema());
        variantColumns.add(variantColumn);
        return variantColumn;
      }
      HoodieSchema output = outputSchema.getNonNullType();
      HoodieSchema file = fileSchema.getNonNullType();
      if (output.getType() != file.getType() || parquetType == null || parquetType.isPrimitive()) {
        return null;
      }
      GroupType group = parquetType.asGroupType();
      switch (output.getType()) {
        case RECORD: {
          List<HoodieSchemaField> outputFields = output.getFields();
          Rebuilder[] fieldRebuilders = new Rebuilder[outputFields.size()];
          boolean anyTarget = false;
          for (int i = 0; i < outputFields.size(); i++) {
            HoodieSchemaField outputField = outputFields.get(i);
            Option<HoodieSchemaField> fileField = file.getField(outputField.name());
            fieldRebuilders[i] = fileField.isPresent()
                ? plan(outputField.schema(), fileField.get().schema(), childType(group, outputField.name()),
                    path == null ? outputField.name() : path + "." + outputField.name())
                : null;
            anyTarget |= fieldRebuilders[i] != null;
          }
          return anyTarget ? new RecordRebuilder(output.getAvroSchema(), fieldRebuilders) : null;
        }
        case ARRAY: {
          Rebuilder elementRebuilder = plan(output.getElementType(), file.getElementType(),
              listElementType(group), path + ".element");
          return elementRebuilder == null ? null : new ArrayRebuilder(output.getAvroSchema(), elementRebuilder);
        }
        case MAP: {
          Rebuilder valueRebuilder = plan(output.getValueType(), file.getValueType(),
              mapValueType(group), path + ".value");
          return valueRebuilder == null ? null : new MapRebuilder(valueRebuilder);
        }
        default:
          return null;
      }
    }

    /**
     * The element type of a parquet LIST group, by the same rules the file schema's Avro conversion
     * applied (AvroSchemaConverter.isElementType): the repeated field itself in the legacy 2-level
     * layouts, otherwise the single field of the repeated group (3-level layout).
     */
    private Type listElementType(GroupType list) {
      if (list.getFieldCount() != 1) {
        return null;
      }
      Type repeated = list.getType(0);
      boolean repeatedIsElement = repeated.isPrimitive()
          || repeated.asGroupType().getFieldCount() > 1
          || repeated.asGroupType().getType(0).isRepetition(Type.Repetition.REPEATED)
          || repeated.getName().equals("array")
          || repeated.getName().equals(list.getName() + "_tuple")
          || assumeRepeatedIsListElement;
      return repeatedIsElement ? repeated : repeated.asGroupType().getType(0);
    }

    /** The value type of a parquet MAP group: the second field of its repeated key_value group. */
    private static Type mapValueType(GroupType map) {
      if (map.getFieldCount() != 1 || map.getType(0).isPrimitive()) {
        return null;
      }
      GroupType keyValue = map.getType(0).asGroupType();
      return keyValue.getFieldCount() == 2 ? keyValue.getType(1) : null;
    }

    private static Type childType(GroupType group, String name) {
      return group.containsField(name) ? group.getType(name) : null;
    }
  }

  /** Rebuilds one value read at the file's shape into its unshredded output shape. */
  private interface Rebuilder {
    Object rebuild(Object value);
  }

  private static final class VariantColumnRebuilder implements Rebuilder {
    private final String path;
    private final GroupType parquetType;
    private final Schema unshreddedSchema;
    // Set by prepare() once the provider is loaded, before any record is rebuilt.
    private VariantShreddingProvider.VariantRebuilder rebuilder;

    private VariantColumnRebuilder(String path, GroupType parquetType, Schema unshreddedSchema) {
      this.path = path;
      this.parquetType = parquetType;
      this.unshreddedSchema = unshreddedSchema;
    }

    private void prepare(VariantShreddingProvider provider) {
      rebuilder = provider.prepareRebuild(path, parquetType, unshreddedSchema);
    }

    @Override
    public Object rebuild(Object value) {
      // A null variant passes through unchanged.
      return value instanceof GenericRecord ? rebuilder.rebuild((GenericRecord) value) : value;
    }
  }

  private static final class RecordRebuilder implements Rebuilder {
    private final Schema outputSchema;
    // Indexed by field position in the (output == intermediate) record; null for non-targets.
    private final Rebuilder[] fieldRebuilders;

    private RecordRebuilder(Schema outputSchema, Rebuilder[] fieldRebuilders) {
      this.outputSchema = outputSchema;
      this.fieldRebuilders = fieldRebuilders;
    }

    @Override
    public Object rebuild(Object value) {
      if (!(value instanceof IndexedRecord)) {
        return value;
      }
      IndexedRecord in = (IndexedRecord) value;
      GenericRecord out = new GenericData.Record(outputSchema);
      for (int i = 0; i < fieldRebuilders.length; i++) {
        Object fieldValue = in.get(i);
        out.put(i, fieldRebuilders[i] == null ? fieldValue : fieldRebuilders[i].rebuild(fieldValue));
      }
      return out;
    }
  }

  private static final class ArrayRebuilder implements Rebuilder {
    private final Schema outputSchema;
    private final Rebuilder elementRebuilder;

    private ArrayRebuilder(Schema outputSchema, Rebuilder elementRebuilder) {
      this.outputSchema = outputSchema;
      this.elementRebuilder = elementRebuilder;
    }

    @Override
    public Object rebuild(Object value) {
      if (!(value instanceof List)) {
        return value;
      }
      List<?> in = (List<?>) value;
      GenericData.Array<Object> out = new GenericData.Array<>(in.size(), outputSchema);
      for (Object element : in) {
        out.add(elementRebuilder.rebuild(element));
      }
      return out;
    }
  }

  private static final class MapRebuilder implements Rebuilder {
    private final Rebuilder valueRebuilder;

    private MapRebuilder(Rebuilder valueRebuilder) {
      this.valueRebuilder = valueRebuilder;
    }

    @Override
    public Object rebuild(Object value) {
      if (!(value instanceof Map)) {
        return value;
      }
      Map<?, ?> in = (Map<?, ?>) value;
      Map<Object, Object> out = new LinkedHashMap<>(in.size());
      for (Map.Entry<?, ?> entry : in.entrySet()) {
        out.put(entry.getKey(), valueRebuilder.rebuild(entry.getValue()));
      }
      return out;
    }
  }

  private static VariantShreddingProvider loadProvider(HoodieStorage storage) {
    String providerClass = storage.getConf()
        .getString(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_PROVIDER_CLASS.key()).orElse(null);
    if (providerClass == null || providerClass.isEmpty()) {
      providerClass = VariantShreddingRuntime.getProviderClass().orElse(null);
    }
    return providerClass == null ? null : (VariantShreddingProvider) ReflectionUtils.loadClass(providerClass);
  }
}
