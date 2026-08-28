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

package org.apache.hudi.common.avro;

import org.apache.hudi.common.config.HoodieConfig;
import org.apache.hudi.common.config.HoodieStorageConfig;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Shared helpers for converting between shredded and unshredded variant schemas.
 * Used by both the write path ({@code HoodieAvroWriteSupport}) and the read path
 * (variant reconstruction in the parquet reader).
 */
public class VariantSchemaUtils {

  // Defined in hudi-client's HoodieWriteConfig; referenced literally because hudi-common cannot
  // depend on it (same precedent as "hoodie.index.type" in HoodieFileWriterFactory).
  private static final String INTERNAL_SCHEMA_KEY = "hoodie.internal.schema";
  private static final String WRITE_SCHEMA_OVERRIDE_KEY = "hoodie.write.schema";
  private static final String AVRO_SCHEMA_KEY = "hoodie.avro.schema";

  /**
   * Namespace of the shredded record types {@link #applyInferredShredding} generates. Hudi-owned
   * so the generated names ({@code <column>_variant}) cannot collide with a user-declared record
   * type of the same simple name in the table schema. Deliberately distinct from
   * {@link #FORCED_VARIANT_NAMESPACE}: the two splices generate the same simple name for the same
   * column, so a shared namespace would let an inferred record and a forced one collide on one
   * full name while carrying different typed_value schemas.
   */
  private static final String INFERRED_VARIANT_NAMESPACE = "hoodie.variant.inferred";

  /**
   * Namespace prefix of the shredded record types {@link #applyForcedShredding} generates; the
   * enclosing record's full name is appended, so a forced variant is named
   * {@code hoodie.variant.forced.<enclosing record full name>.<field>_variant}.
   *
   * <p>The name has to be a function of the (enclosing record type, field name) pair and of
   * nothing else. {@code Schema.toString()}, which is what gets stamped into the parquet footer as
   * {@code parquet.avro.schema}, throws "Can't redefine" when two NON-equal record definitions
   * share a full name, and emits the second of two EQUAL ones as a bare name reference. A record
   * type reused under two fields is rebuilt once per position by this splice, so the two rebuilds
   * must come out EQUAL; keying the generated name on the enclosing record TYPE does that, whereas
   * a dotted-path name ({@code a.v} against {@code b.v}) would make them differ and fail at file
   * open. Two distinct record types with a same-named variant member still land in distinct
   * namespaces, which is exactly Avro's own notion of identity.
   *
   * <p>The records generated under the variant carry this namespace too, so a DDL field name is
   * free to match a user-declared record type in the table schema: the {@code typed_value} record
   * sits directly in it, and the {@code {value, typed_value}} struct per DDL field one level below
   * that, under the {@code typed_value} record itself (see
   * {@link HoodieSchema#createShreddedFieldStruct(String, String, HoodieSchema)} for why a DDL
   * field spelled {@code typed_value} or {@code <column>_variant} needs the extra level).
   */
  private static final String FORCED_VARIANT_NAMESPACE = "hoodie.variant.forced";

  private VariantSchemaUtils() {
  }

  /**
   * Strips shredding from the variant fields in {@code schema}, replacing each shredded variant
   * with its unshredded form (dropping {@code typed_value}). Variants nested inside records, array
   * elements and map values are stripped too; see {@link #swapShreddedVariantFields} for when such
   * a schema can arise. Non-variant fields and already-unshredded variants pass through unchanged;
   * returns {@code schema} as-is when nothing changes.
   *
   * <p>Every field of a rebuilt record is copied via {@code withSchema}, including the untouched
   * ones: reusing an Avro {@code Field} still bound to the source record makes
   * {@code Schema.setFields} throw "Field already used" (the defect #18938 fixed in the sibling
   * HoodieVariantReconstruction).
   */
  public static HoodieSchema stripVariantShredding(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      return schema;
    }
    return stripRecordVariantShredding(schema);
  }

  private static HoodieSchema stripRecordVariantShredding(HoodieSchema record) {
    List<HoodieSchemaField> fields = record.getFields();
    // Built lazily: every schema without a shredded variant walks this method, and copying fields
    // only to discard them costs an Avro Field plus a defaultVal() lookup per field per level.
    List<HoodieSchemaField> newFields = null;
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchemaField field = fields.get(i);
      HoodieSchema fieldSchema = field.schema();
      HoodieSchema replacement = stripVariantShreddingAt(fieldSchema);
      if (replacement != fieldSchema && newFields == null) {
        newFields = copyFieldsBefore(fields, i);
      }
      if (newFields != null) {
        // withSchema makes a fresh Avro Field: reusing one already bound to this record would fail
        // Schema.setFields with "Field already used" when building the replacement record below.
        newFields.add(field.withSchema(replacement));
      }
    }
    if (newFields == null) {
      return record;
    }
    return HoodieSchema.createRecord(
        record.getAvroSchema().getName(),
        record.getAvroSchema().getNamespace(),
        record.getAvroSchema().getDoc(),
        newFields);
  }

  /** Strips shredding at one schema position, returning the argument instance when nothing changes. */
  private static HoodieSchema stripVariantShreddingAt(HoodieSchema schema) {
    boolean wasNullable = schema.isNullable();
    HoodieSchema unwrapped = wasNullable ? schema.getNonNullType() : schema;
    HoodieSchema replacement;
    switch (unwrapped.getType()) {
      case VARIANT:
        if (!((HoodieSchema.Variant) unwrapped).isShredded()) {
          return schema;
        }
        replacement = HoodieSchema.createVariant(
            unwrapped.getAvroSchema().getName(),
            unwrapped.getAvroSchema().getNamespace(),
            unwrapped.getAvroSchema().getDoc());
        break;
      case RECORD:
        replacement = stripRecordVariantShredding(unwrapped);
        break;
      case ARRAY: {
        HoodieSchema elementType = unwrapped.getElementType();
        HoodieSchema strippedElement = stripVariantShreddingAt(elementType);
        replacement = strippedElement == elementType ? unwrapped : HoodieSchema.createArray(strippedElement);
        break;
      }
      case MAP: {
        HoodieSchema valueType = unwrapped.getValueType();
        HoodieSchema strippedValue = stripVariantShreddingAt(valueType);
        replacement = strippedValue == valueType ? unwrapped : HoodieSchema.createMap(strippedValue);
        break;
      }
      default:
        return schema;
    }
    if (replacement == unwrapped) {
      return schema;
    }
    return wasNullable ? HoodieSchema.createNullable(replacement) : replacement;
  }

  /**
   * Splices the forced test-DDL shredding schema into every variant that is a RECORD MEMBER of
   * {@code schema}, at any depth: top-level fields, members of nested records, and members of
   * records reached through array elements and map values. The inverse direction of
   * {@link #stripVariantShredding}. Shredded and unshredded variants alike are replaced, because
   * the DDL overrides whatever the write schema declared. Returns {@code schema} as-is when it is
   * not a record, when {@code typedValueFields} is empty, or when nothing below it is a variant.
   *
   * <p>A variant that is DIRECTLY an array element or a map value is deliberately NOT forced,
   * mirroring {@code HoodieRowParquetWriteSupport}: its forced-DDL arm sits in
   * {@code generateShreddedSchema}, which walks record fields, while the array and map arms of
   * {@code processNestedDataType} shred an element or value only when the write schema itself
   * declares a {@code typed_value} there. Both write supports keeping the same reach is what makes
   * a table's on-disk layout independent of the record type it was written with.
   *
   * <p>Value-level shredding is schema-driven at any position ({@code HoodieAvroWriteSupport}
   * walks the effective schema this method returns), so a hand-authored write schema that declares
   * {@code typed_value} on a bare element or value still shreds; only this DDL hook stops at
   * record members.
   *
   * @param schema           the writer schema
   * @param typedValueFields the DDL fields (name to type) every forced variant is shredded on, as
   *                         parsed from {@code hoodie.parquet.variant.force.shredding.schema.for.test}
   */
  public static HoodieSchema applyForcedShredding(HoodieSchema schema, Map<String, HoodieSchema> typedValueFields) {
    if (schema.getType() != HoodieSchemaType.RECORD || typedValueFields == null || typedValueFields.isEmpty()) {
      return schema;
    }
    return applyForcedShreddingToRecord(schema, typedValueFields);
  }

  private static HoodieSchema applyForcedShreddingToRecord(HoodieSchema record, Map<String, HoodieSchema> typedValueFields) {
    List<HoodieSchemaField> fields = record.getFields();
    // Built lazily, as in stripRecordVariantShredding: every record without a variant member walks
    // this method and must not pay for a field copy it will throw away.
    List<HoodieSchemaField> newFields = null;
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchemaField field = fields.get(i);
      HoodieSchema fieldSchema = field.schema();
      // A record member is the one position the DDL is allowed to shred; see applyForcedShredding.
      HoodieSchema replacement = applyForcedShreddingAt(fieldSchema, typedValueFields, record, field.name(), true);
      if (replacement != fieldSchema && newFields == null) {
        newFields = copyFieldsBefore(fields, i);
      }
      if (newFields != null) {
        // withSchema makes a fresh Avro Field: reusing one already bound to this record would fail
        // Schema.setFields with "Field already used" when building the replacement record below.
        newFields.add(field.withSchema(replacement));
      }
    }
    if (newFields == null) {
      return record;
    }
    return HoodieSchema.createRecord(
        record.getAvroSchema().getName(),
        record.getAvroSchema().getNamespace(),
        record.getAvroSchema().getDoc(),
        newFields);
  }

  /**
   * Applies the forced DDL at one schema position, returning the argument instance when nothing
   * changes. {@code enclosingRecord} and {@code fieldName} are the record member this position was
   * reached from and name the generated record (see {@link #FORCED_VARIANT_NAMESPACE}).
   * {@code variantAllowed} is false once the walk has stepped through an array element or a map
   * value, where the DDL does not reach; it goes back to true for the members of any record found
   * there, so {@code array<struct<v variant>>} shreds while {@code array<variant>} does not.
   *
   * <p>A genuine multi-branch UNION is out of scope. {@code getNonNullType} only unwraps the
   * nullable two-branch form, so anything else comes back a UNION, hits {@code default} and passes
   * through untouched -- a variant inside it is never forced, at any depth below it.
   * {@code HoodieAvroWriteSupport.buildShredder} has the same arm, so the schema splice and the
   * value walk agree on what a union does; a future change to either has to move both.
   */
  private static HoodieSchema applyForcedShreddingAt(HoodieSchema schema,
                                                     Map<String, HoodieSchema> typedValueFields,
                                                     HoodieSchema enclosingRecord,
                                                     String fieldName,
                                                     boolean variantAllowed) {
    boolean wasNullable = schema.isNullable();
    HoodieSchema unwrapped = wasNullable ? schema.getNonNullType() : schema;
    HoodieSchema replacement;
    switch (unwrapped.getType()) {
      case VARIANT:
        if (!variantAllowed) {
          return schema;
        }
        replacement = HoodieSchema.createVariantShreddedObject(
            fieldName + "_variant",
            FORCED_VARIANT_NAMESPACE + "." + enclosingRecord.getFullName(),
            unwrapped.getAvroSchema().getDoc(),
            typedValueFields);
        break;
      case RECORD:
        replacement = applyForcedShreddingToRecord(unwrapped, typedValueFields);
        break;
      case ARRAY: {
        HoodieSchema elementType = unwrapped.getElementType();
        HoodieSchema shreddedElement = applyForcedShreddingAt(elementType, typedValueFields, enclosingRecord, fieldName, false);
        replacement = shreddedElement == elementType ? unwrapped : HoodieSchema.createArray(shreddedElement);
        break;
      }
      case MAP: {
        HoodieSchema valueType = unwrapped.getValueType();
        HoodieSchema shreddedValue = applyForcedShreddingAt(valueType, typedValueFields, enclosingRecord, fieldName, false);
        replacement = shreddedValue == valueType ? unwrapped : HoodieSchema.createMap(shreddedValue);
        break;
      }
      default:
        return schema;
    }
    if (replacement == unwrapped) {
      return schema;
    }
    return wasNullable ? HoodieSchema.createNullable(replacement) : replacement;
  }

  /**
   * Strips {@code typed_value} from fields that have the variant SHAPE but lost the variant
   * logical type, i.e. plain records of {@code {metadata: bytes, value: [nullable] bytes,
   * typed_value}} (see {@link #isShreddedVariantShape}). Parquet-footer-derived schemas come back
   * this way (the converter does not attach the variant logical type), so
   * {@link #stripVariantShredding} alone cannot see them. Used by the table-schema footer
   * fallback only; returns {@code schema} as-is when nothing matches.
   *
   * <p>The walk recurses through records, array elements and map values because both write
   * supports shred below the top level: the forced-shredding DDL reaches every variant that is a
   * record member at any depth on the ROW path
   * ({@code HoodieRowParquetWriteSupport.processNestedDataType}) and on the AVRO path
   * ({@link #applyForcedShredding}), and a write schema that declares {@code typed_value} on a
   * bare array element or map value shreds it on either path.
   *
   * <p>Unlike {@link #isShreddedVariantTarget}, the match here has NO requested-side anchor:
   * the footer fallback runs precisely when no table schema is available to anchor on, so a
   * plain user struct that happens to have exactly this shape is stripped too (a documented,
   * accepted false positive: {@code metadata} plus {@code typed_value} is the variant spec's
   * vocabulary). Recursing extends that false positive from top-level fields to every depth. A
   * match is terminal: the members of a variant group are the spec's own fields, not user columns
   * that could hold a nested variant of their own.
   *
   * <p>The shape check also admits the spec's two-field {@code {metadata, typed_value}} form (a
   * writer may omit {@code value} when every row is typed). Stripping that would leave a
   * one-field record, so {@code value} is restored as nullable bytes: the result is always the
   * unshredded {@code {metadata, value}} shape.
   */
  public static HoodieSchema stripVariantShreddingByShape(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      return schema;
    }
    return stripRecordVariantShreddingByShape(schema);
  }

  private static HoodieSchema stripRecordVariantShreddingByShape(HoodieSchema record) {
    List<HoodieSchemaField> fields = record.getFields();
    // Built lazily, as in stripRecordVariantShredding: this fallback runs on every table-schema
    // resolution from a data file, and the overwhelmingly common schema matches nothing.
    List<HoodieSchemaField> newFields = null;
    for (int i = 0; i < fields.size(); i++) {
      HoodieSchemaField field = fields.get(i);
      HoodieSchema fieldSchema = field.schema();
      HoodieSchema replacement = stripVariantShreddingByShapeAt(fieldSchema);
      if (replacement != fieldSchema && newFields == null) {
        newFields = copyFieldsBefore(fields, i);
      }
      if (newFields != null) {
        // withSchema makes a fresh Avro Field: reusing one already bound to this record would fail
        // Schema.setFields with "Field already used" when building the replacement record below.
        newFields.add(field.withSchema(replacement));
      }
    }
    if (newFields == null) {
      return record;
    }
    return HoodieSchema.createRecord(
        record.getAvroSchema().getName(),
        record.getAvroSchema().getNamespace(),
        record.getAvroSchema().getDoc(),
        newFields);
  }

  /** Strips by shape at one schema position, returning the argument instance when nothing changes. */
  private static HoodieSchema stripVariantShreddingByShapeAt(HoodieSchema schema) {
    boolean wasNullable = schema.isNullable();
    HoodieSchema unwrapped = wasNullable ? schema.getNonNullType() : schema;
    HoodieSchema replacement;
    switch (unwrapped.getType()) {
      case RECORD:
        // A match is terminal: whatever sits under a variant group belongs to the shredding spec,
        // not to the user, so the walk never descends into one.
        replacement = isShreddedVariantShape(unwrapped)
            ? stripShreddedVariantShape(unwrapped)
            : stripRecordVariantShreddingByShape(unwrapped);
        break;
      case ARRAY: {
        HoodieSchema elementType = unwrapped.getElementType();
        HoodieSchema strippedElement = stripVariantShreddingByShapeAt(elementType);
        replacement = strippedElement == elementType ? unwrapped : HoodieSchema.createArray(strippedElement);
        break;
      }
      case MAP: {
        HoodieSchema valueType = unwrapped.getValueType();
        HoodieSchema strippedValue = stripVariantShreddingByShapeAt(valueType);
        replacement = strippedValue == valueType ? unwrapped : HoodieSchema.createMap(strippedValue);
        break;
      }
      default:
        return schema;
    }
    if (replacement == unwrapped) {
      return schema;
    }
    return wasNullable ? HoodieSchema.createNullable(replacement) : replacement;
  }

  /** Rebuilds one shape-matched variant group in the unshredded {@code {metadata, value}} form. */
  private static HoodieSchema stripShreddedVariantShape(HoodieSchema variantShaped) {
    List<HoodieSchemaField> strippedFields = new ArrayList<>();
    for (HoodieSchemaField member : variantShaped.getFields()) {
      if (!HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD.equals(member.name())) {
        strippedFields.add(HoodieSchemaUtils.createNewSchemaField(member));
      }
    }
    if (!variantShaped.getField(HoodieSchema.Variant.VARIANT_VALUE_FIELD).isPresent()) {
      // Null default, like every other optional field on this path (the footer converter
      // attaches one, and kept members carry theirs over): HoodieSchemaField#equals compares
      // defaults, so omitting it would make the stripped schema differ from the one a
      // three-field file yields for the same column.
      strippedFields.add(HoodieSchemaField.of(
          HoodieSchema.Variant.VARIANT_VALUE_FIELD, HoodieSchema.createNullable(HoodieSchemaType.BYTES),
          null, HoodieSchema.NULL_VALUE));
    }
    return HoodieSchema.createRecord(
        variantShaped.getAvroSchema().getName(),
        variantShaped.getAvroSchema().getNamespace(),
        variantShaped.getAvroSchema().getDoc(),
        strippedFields);
  }

  /**
   * The top-level variant columns of {@code schema} that are candidates for shredding-schema
   * inference under {@code config}, or an empty list when inference does not apply.
   *
   * <p>Inference applies only when it is enabled, write shredding is enabled, no forced test
   * DDL is set (force wins), and no internal schema is set (the Spark write support prefers the
   * internal schema and would silently ignore an inferred one). Candidate columns are top-level
   * unshredded variants; columns with an explicit typed_value keep their schema-driven shredding.</p>
   */
  public static List<String> getInferableVariantColumns(HoodieConfig config, HoodieSchema schema) {
    return isShreddingInferenceEnabled(config) ? unshreddedTopLevelVariantColumns(schema) : Collections.emptyList();
  }

  /** The top-level unshredded variant columns of {@code schema}; empty when it is not a record. */
  private static List<String> unshreddedTopLevelVariantColumns(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      return Collections.emptyList();
    }
    List<String> columns = new ArrayList<>();
    for (HoodieSchemaField field : schema.getFields()) {
      HoodieSchema unwrapped = field.schema().isNullable() ? field.schema().getNonNullType() : field.schema();
      if (unwrapped.getType() == HoodieSchemaType.VARIANT && !((HoodieSchema.Variant) unwrapped).isShredded()) {
        columns.add(field.name());
      }
    }
    return columns;
  }

  /**
   * {@link #getInferableVariantColumns} over the schema the config carries (see
   * {@link #getConfigWriteSchema}), for write supports that resolve their schema from the config
   * rather than a schema argument: detecting on that same schema guarantees the detection and the
   * {@link #applyInferredShreddingToConfig} splice agree. Empty when inference does not apply or
   * no schema is set; the schema is only parsed once the cheap config gates pass.
   */
  public static List<String> getInferableVariantColumnsFromConfig(HoodieConfig config) {
    if (!isShreddingInferenceEnabled(config)) {
      return Collections.emptyList();
    }
    return getConfigWriteSchema(config)
        .map(VariantSchemaUtils::unshreddedTopLevelVariantColumns)
        .orElse(Collections.emptyList());
  }

  /**
   * The write schema carried by the config, {@code hoodie.write.schema} else
   * {@code hoodie.avro.schema}, in that precedence (the one the Spark row write support resolves
   * and the one {@link #applyInferredShreddingToConfig} splices into); empty when neither is set.
   */
  public static Option<HoodieSchema> getConfigWriteSchema(HoodieConfig config) {
    String schemaString = config.getString(WRITE_SCHEMA_OVERRIDE_KEY);
    if (StringUtils.isNullOrEmpty(schemaString)) {
      schemaString = config.getString(AVRO_SCHEMA_KEY);
    }
    return StringUtils.isNullOrEmpty(schemaString) ? Option.empty() : Option.of(HoodieSchema.parse(schemaString));
  }

  /**
   * Whether shredding-schema inference applies under {@code config}: enabled, write shredding
   * enabled, no forced test DDL (force wins), and no internal schema (the Spark write support
   * prefers the internal schema and would silently ignore an inferred one).
   */
  private static boolean isShreddingInferenceEnabled(HoodieConfig config) {
    return config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_VARIANT_SHREDDING_SCHEMA_INFERENCE_ENABLED)
        && config.getBooleanOrDefault(HoodieStorageConfig.PARQUET_VARIANT_WRITE_SHREDDING_ENABLED)
        && StringUtils.isNullOrEmpty(config.getString(HoodieStorageConfig.PARQUET_VARIANT_FORCE_SHREDDING_SCHEMA_FOR_TEST))
        && StringUtils.isNullOrEmpty(config.getString(INTERNAL_SCHEMA_KEY));
  }

  /**
   * Returns a copy of {@code config} with the inferred typed_value schemas spliced into its
   * schema-carrying keys ({@code hoodie.write.schema}, {@code hoodie.avro.schema}), or
   * {@code config} itself when there is nothing to splice. Used for write supports that resolve
   * their schema from the config rather than a schema argument (the Spark row write support);
   * the original config is never mutated.
   */
  public static HoodieConfig applyInferredShreddingToConfig(HoodieConfig config, Map<String, HoodieSchema> typedValueByField) {
    if (typedValueByField.isEmpty()) {
      return config;
    }
    HoodieConfig copy = new HoodieConfig(TypedProperties.copy(config.getProps()));
    for (String key : new String[] {WRITE_SCHEMA_OVERRIDE_KEY, AVRO_SCHEMA_KEY}) {
      String schemaString = copy.getString(key);
      if (!StringUtils.isNullOrEmpty(schemaString)) {
        HoodieSchema spliced = applyInferredShredding(HoodieSchema.parse(schemaString), typedValueByField);
        copy.setValue(key, spliced.getAvroSchema().toString());
      }
    }
    return copy;
  }

  /**
   * Splices inferred typed_value schemas into the matching top-level variant fields of
   * {@code schema}; the inverse direction of {@link #stripVariantShredding}. Only unshredded
   * variant fields with an entry in {@code typedValueByField} are replaced; everything else
   * passes through unchanged, and {@code schema} is returned as-is when nothing matches.
   *
   * @param schema            the writer schema
   * @param typedValueByField column name to typed_value schema in the nested shredding-spec
   *                          form, as produced by a {@link VariantShreddingSchemaInferrer}
   */
  public static HoodieSchema applyInferredShredding(HoodieSchema schema, Map<String, HoodieSchema> typedValueByField) {
    if (schema.getType() != HoodieSchemaType.RECORD || typedValueByField.isEmpty()) {
      return schema;
    }

    List<HoodieSchemaField> newFields = new ArrayList<>();
    boolean changed = false;

    for (HoodieSchemaField field : schema.getFields()) {
      HoodieSchema fieldSchema = field.schema();
      boolean wasNullable = fieldSchema.isNullable();
      HoodieSchema unwrapped = wasNullable ? fieldSchema.getNonNullType() : fieldSchema;
      HoodieSchema typedValue = typedValueByField.get(field.name());

      if (typedValue != null
          && unwrapped.getType() == HoodieSchemaType.VARIANT
          && !((HoodieSchema.Variant) unwrapped).isShredded()) {
        // typed_value must be nullable: rows whose variant value does not match the inferred
        // schema are written with a null typed_value and the full binary in the value column.
        HoodieSchema nullableTypedValue = typedValue.isNullable() ? typedValue : HoodieSchema.createNullable(typedValue);
        // The shredded record needs a name unique to this column: variant columns commonly share
        // one record type (named "variant"), which Avro serializes as name references after the
        // first occurrence. Reusing the shared name would alias every other variant column to
        // this column's shredded definition once the schema is serialized (the config-splice
        // path), and two inferred columns could not coexist in one schema. The Hudi-owned
        // namespace keeps the generated name clear of any user-declared type as well.
        HoodieSchema.Variant shredded = HoodieSchema.createVariantShredded(
            field.name() + "_variant",
            INFERRED_VARIANT_NAMESPACE,
            unwrapped.getAvroSchema().getDoc(),
            nullableTypedValue);
        HoodieSchema replacement = wasNullable ? HoodieSchema.createNullable(shredded) : shredded;
        // createNewSchemaField: the existing avro fields are attached to the source schema and
        // cannot be reused in a new record ("Field already used").
        newFields.add(HoodieSchemaUtils.createNewSchemaField(field.withSchema(replacement)));
        changed = true;
      } else {
        newFields.add(HoodieSchemaUtils.createNewSchemaField(field));
      }
    }

    if (!changed) {
      return schema;
    }

    return HoodieSchema.createRecord(
        schema.getAvroSchema().getName(),
        schema.getAvroSchema().getNamespace(),
        schema.getAvroSchema().getDoc(),
        newFields);
  }

  /**
   * Whether this column sits shredded on disk and must be read in that shape and reconstructed to
   * serve the requested schema. A file schema that kept its variant logical type answers via
   * {@link HoodieSchema.Variant#isShredded()}; but a file schema derived from converting the
   * parquet footer MessageType loses the logical type (variant groups come back as plain records),
   * so the on-disk side is detected by SHAPE, anchored by the requested side: the requested column
   * (from the table schema, logical type intact) must be a variant for the shape match to count,
   * leaving plain user structs of the same shape alone (#19567).
   *
   * @param fileFieldSchema      the column as it sits in the file schema
   * @param requestedFieldSchema the same column as requested, carrying the variant logical type
   */
  public static boolean isShreddedVariantTarget(HoodieSchema fileFieldSchema, HoodieSchema requestedFieldSchema) {
    HoodieSchema file = fileFieldSchema.getNonNullType();
    if (file.getType() == HoodieSchemaType.VARIANT && ((HoodieSchema.Variant) file).isShredded()) {
      return true;
    }
    HoodieSchema requested = requestedFieldSchema.getNonNullType();
    return requested.getType() == HoodieSchemaType.VARIANT && isShreddedVariantShape(file);
  }

  /**
   * Returns {@code fileSchema} with each shredded variant column (per
   * {@link #isShreddedVariantTarget}) replaced by its requested counterpart, for projection or
   * compatibility checks against {@code requestedSchema}. A footer-derived shredded variant column
   * surfaces as a plain {@code {metadata, value, typed_value}} record and so can never look like a
   * projection source of the requested variant, even though the readers reconstruct it (see
   * HoodieVariantReconstruction). Returns {@code fileSchema} as-is when nothing matches.
   */
  public static HoodieSchema alignShreddedVariants(HoodieSchema fileSchema, HoodieSchema requestedSchema) {
    if (fileSchema.getType() != HoodieSchemaType.RECORD || requestedSchema.getType() != HoodieSchemaType.RECORD) {
      return fileSchema;
    }
    return swapShreddedVariantFields(fileSchema, requestedSchema, true);
  }

  /**
   * The dual of {@link #alignShreddedVariants}: returns {@code requestedSchema} with each shredded
   * variant column swapped to its on-disk (typed_value-bearing) form taken from {@code fileSchema}.
   * This is the schema to read the file at, so parquet materializes {@code typed_value} for the
   * reader to reconstruct from. Returns {@code requestedSchema} as-is when nothing matches, which
   * is how callers detect that the file has no shredded variant column to reconstruct.
   */
  public static HoodieSchema toShreddedReadSchema(HoodieSchema requestedSchema, HoodieSchema fileSchema) {
    if (fileSchema.getType() != HoodieSchemaType.RECORD || requestedSchema.getType() != HoodieSchemaType.RECORD) {
      return requestedSchema;
    }
    return swapShreddedVariantFields(requestedSchema, fileSchema, false);
  }

  /**
   * Walks {@code base} against its matching {@code other} fields by name, replacing every shredded
   * variant position with the other side's schema. {@code baseIsFile} says which of the two is the
   * file side, which is what {@link #isShreddedVariantTarget} needs to anchor detection. Returns
   * {@code base} when nothing matches.
   *
   * <p>The walk recurses through records, array elements and map values because both write
   * supports shred at any depth their write schema asks them to. The forced-shredding DDL
   * ({@code hoodie.parquet.variant.force.shredding.schema.for.test}) force-shreds every variant
   * that is a record member at any depth on the ROW path
   * ({@code HoodieRowParquetWriteSupport.generateShreddedSchema} re-reads the DDL on every entry
   * and {@code processNestedDataType} recurses into structs, array elements and map values) and,
   * since the #19689 fix, on the AVRO path too ({@link #applyForcedShredding}), so
   * {@code struct<v variant>} comes out shredded whichever record type wrote it. A variant that is
   * DIRECTLY an array element or a map value is force-shredded by neither; it shreds on either
   * path only when the write schema itself declares {@code typed_value} there.
   */
  private static HoodieSchema swapShreddedVariantFields(HoodieSchema base, HoodieSchema other, boolean baseIsFile) {
    List<HoodieSchemaField> baseFields = base.getFields();
    // Built lazily, as in stripRecordVariantShredding: alignShreddedVariants runs on every
    // HoodieMergeHelper.runMerge, so the overwhelmingly common case is a schema that matches
    // nothing and must not pay for a full field copy it will throw away.
    List<HoodieSchemaField> newFields = null;
    for (int i = 0; i < baseFields.size(); i++) {
      HoodieSchemaField baseField = baseFields.get(i);
      HoodieSchema baseFieldSchema = baseField.schema();
      Option<HoodieSchemaField> otherField = other.getField(baseField.name());
      HoodieSchema replacement = otherField.isPresent()
          ? swapShreddedVariantsAt(baseFieldSchema, otherField.get().schema(), baseIsFile)
          : baseFieldSchema;
      if (replacement != baseFieldSchema && newFields == null) {
        newFields = copyFieldsBefore(baseFields, i);
      }
      if (newFields != null) {
        // Copy untouched fields too (withSchema makes a fresh Avro Field): reusing a field already
        // bound to the base record would fail Schema.setFields with "Field already used" when
        // building the swapped record below.
        newFields.add(baseField.withSchema(replacement));
      }
    }
    if (newFields == null) {
      return base;
    }
    return HoodieSchema.createRecord(
        base.getAvroSchema().getName(),
        base.getAvroSchema().getNamespace(),
        base.getAvroSchema().getDoc(),
        newFields);
  }

  /** Swaps at one schema position, returning the {@code base} instance when nothing changes. */
  private static HoodieSchema swapShreddedVariantsAt(HoodieSchema base, HoodieSchema other, boolean baseIsFile) {
    if (baseIsFile ? isShreddedVariantTarget(base, other) : isShreddedVariantTarget(other, base)) {
      // Take the other side's schema wholesale, nullability included.
      return other;
    }
    boolean wasNullable = base.isNullable();
    HoodieSchema baseInner = wasNullable ? base.getNonNullType() : base;
    HoodieSchema otherInner = other.isNullable() ? other.getNonNullType() : other;
    if (baseInner.getType() != otherInner.getType()) {
      return base;
    }
    HoodieSchema replacement;
    switch (baseInner.getType()) {
      // VARIANT is deliberately absent: a variant that is not a target here is either unshredded or
      // has no requested-side anchor, and its typed_value internals are the provider's business.
      case RECORD:
        replacement = swapShreddedVariantFields(baseInner, otherInner, baseIsFile);
        break;
      case ARRAY: {
        HoodieSchema baseElement = baseInner.getElementType();
        HoodieSchema swappedElement = swapShreddedVariantsAt(baseElement, otherInner.getElementType(), baseIsFile);
        replacement = swappedElement == baseElement ? baseInner : HoodieSchema.createArray(swappedElement);
        break;
      }
      case MAP: {
        HoodieSchema baseValue = baseInner.getValueType();
        HoodieSchema swappedValue = swapShreddedVariantsAt(baseValue, otherInner.getValueType(), baseIsFile);
        replacement = swappedValue == baseValue ? baseInner : HoodieSchema.createMap(swappedValue);
        break;
      }
      default:
        return base;
    }
    if (replacement == baseInner) {
      return base;
    }
    return wasNullable ? HoodieSchema.createNullable(replacement) : replacement;
  }

  /**
   * The on-disk shredded variant shape: a record of {metadata: bytes, typed_value} plus an optional
   * {value: [nullable] bytes}, and nothing else.
   *
   * <p>{@code value} is deliberately optional. The shredding spec lets a writer omit it when every
   * row is typed, and {@link HoodieSchema.Variant#determineIfShredded} - the answer used whenever
   * the logical type survives - calls anything with a {@code typed_value} shredded regardless. Since
   * the footer always strips the logical type, this shape check is the only detector that runs on
   * real files, so demanding {@code value} here made a two-field group read at the unshredded schema
   * and silently drop its payload: #19567 again by another shape. The requested-side variant anchor
   * in {@link #isShreddedVariantTarget} is what keeps plain user structs out, so accepting the
   * two-field form costs no false positives.
   */
  private static boolean isShreddedVariantShape(HoodieSchema schema) {
    if (schema.getType() != HoodieSchemaType.RECORD) {
      return false;
    }
    int fieldCount = schema.getFields().size();
    if (fieldCount < 2 || fieldCount > 3) {
      return false;
    }
    if (!schema.getField(HoodieSchema.Variant.VARIANT_TYPED_VALUE_FIELD).isPresent()) {
      return false;
    }
    if (!isBytesField(schema, HoodieSchema.Variant.VARIANT_METADATA_FIELD)) {
      return false;
    }
    boolean hasValue = schema.getField(HoodieSchema.Variant.VARIANT_VALUE_FIELD).isPresent();
    // A third field that is not `value` is some other user struct, not a variant group.
    return hasValue
        ? isBytesField(schema, HoodieSchema.Variant.VARIANT_VALUE_FIELD)
        : fieldCount == 2;
  }

  /**
   * Fresh copies of {@code fields[0, end)}, for the point a rebuild first turns out to be needed.
   * Copies rather than reuses because the originals are still bound to their source record.
   */
  private static List<HoodieSchemaField> copyFieldsBefore(List<HoodieSchemaField> fields, int end) {
    List<HoodieSchemaField> copied = new ArrayList<>(fields.size());
    for (int i = 0; i < end; i++) {
      HoodieSchemaField field = fields.get(i);
      copied.add(field.withSchema(field.schema()));
    }
    return copied;
  }

  private static boolean isBytesField(HoodieSchema schema, String fieldName) {
    return schema.getField(fieldName)
        .map(HoodieSchemaField::schema)
        .map(s -> s.isNullable() ? s.getNonNullType() : s)
        .map(s -> s.getType() == HoodieSchemaType.BYTES)
        .orElse(false);
  }
}
