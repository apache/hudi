/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.hudi.util;

import com.google.common.collect.ImmutableList;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.TimeType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarbinaryType;
import io.trino.spi.type.VarcharType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchemaField;
import org.apache.hudi.common.schema.HoodieSchemaType;
import org.apache.hudi.common.schema.HoodieSchemaUtils;

import java.util.List;
import java.util.Locale;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static java.lang.String.format;

/**
 * Converts a Trino column list into the {@link HoodieSchema} that becomes a table's
 * {@code hoodie.table.create.schema}.
 * <p>
 * This direction did not previously exist in the connector: the read path only maps Hudi/Avro
 * types into Trino types. The mapping here is the inverse of Trino's own
 * {@code NativeLogicalTypesAvroTypeManager}, so a column created through this converter reads back
 * as the same Trino type -- except where a mapping is deliberately widening, noted per case below.
 * <p>
 * Mappings that widen, so a column does not read back as the type it was declared with:
 * <ul>
 *   <li>{@code TINYINT} and {@code SMALLINT} become Avro {@code int} and read back as
 *       {@code INTEGER}. Avro has no narrower integer, and Spark's Avro conversion widens the same
 *       way, so rejecting them would make the connector stricter than its peers for no gain.</li>
 *   <li>{@code VARCHAR(n)} becomes Avro {@code string} and reads back as unbounded {@code VARCHAR}.
 *       Avro strings carry no length bound; nothing enforces {@code n} once another engine writes.</li>
 *   <li>{@code TIMESTAMP(p)} for a {@code p} that is not exactly 3 or 6 rounds up to the next
 *       representable precision, since Avro offers only millisecond and microsecond logical types.
 *       The widening is lossless. Separately, the Hive Metastore's {@code timestamp} carries no
 *       precision at all, so every timestamp column reads back at the precision the connector
 *       requests from the metastore rather than the one it was declared with.</li>
 * </ul>
 * Types rejected outright, because the alternative is a mapping that is quietly wrong rather than
 * merely wider:
 * <ul>
 *   <li>{@code CHAR(n)} -- Avro has no fixed-width string, so the blank-padding semantics that
 *       distinguish {@code CHAR} from {@code VARCHAR} would be silently dropped.</li>
 *   <li>{@code TIMESTAMP(p) WITH TIME ZONE} -- Avro's timestamp logical types carry an instant, not
 *       an instant plus a zone, so the per-value zone would be lost.</li>
 *   <li>{@code TIMESTAMP(p)} beyond microsecond precision -- Avro's nanosecond logical types are not
 *       among those the connector's read path decodes, so such a column would be written and then be
 *       unreadable.</li>
 *   <li>{@code UUID} and {@code TIME(p)} -- both have a faithful Avro logical type, but neither has a
 *       Hive counterpart, so {@code HiveTypeTranslator#toHiveType} rejects them and the column could
 *       never be registered in the metastore. Since {@code HudiMetadata#getColumnHandles} reads
 *       columns from the metastore, such a column would also be unreadable. Rejecting here keeps the
 *       failure at the column that caused it.</li>
 *   <li>{@code MAP} with a non-{@code VARCHAR} key type -- Avro map keys are always strings.</li>
 *   <li>Unnamed {@code ROW} fields -- Avro record fields must be named.</li>
 * </ul>
 * {@code DECIMAL} maps to Avro {@code bytes} rather than {@code fixed}. Both are decodable by the
 * read path, and {@code bytes} is what Hudi's own {@link HoodieSchema#createDecimal(int, int)}
 * helper produces; {@code fixed} would additionally require inventing a unique schema name per
 * decimal column, since Avro fixed types are named and must not collide within one schema.
 * <p>
 * Nullability: Trino carries nullability per column but not per element inside a {@code ROW},
 * {@code ARRAY} or {@code MAP}. A column's own {@link ColumnMetadata#isNullable()} is honoured at
 * the top level; everything nested is made nullable, which is what Spark's Avro conversion also
 * produces.
 */
public final class HudiSchemaConverter
{
    private static final String NAMESPACE = "hoodie.trino";
    private static final int MAX_MILLIS_PRECISION = 3;
    private static final int MAX_MICROS_PRECISION = 6;

    private HudiSchemaConverter() {}

    /**
     * Builds the table schema, with Hudi's five meta fields prepended exactly as
     * {@link HoodieSchemaUtils#addMetadataFields} would for any other engine.
     * <p>
     * The returned schema is the single source of truth for both {@code hoodie.table.create.schema}
     * and the Hive Metastore column list. Deriving those two from separate inputs is what produces a
     * table whose metastore descriptor and Hudi schema disagree (HUDI-9435).
     *
     * @param columns all table columns in declaration order, partition columns included
     * @param tableName used to name the Avro record
     */
    public static HoodieSchema toTableSchema(List<ColumnMetadata> columns, String tableName)
    {
        ImmutableList.Builder<HoodieSchemaField> fields = ImmutableList.builder();
        for (ColumnMetadata column : columns) {
            HoodieSchema fieldSchema = toHoodieSchema(column.getType(), column.getName());
            if (column.isNullable()) {
                fields.add(HoodieSchemaField.of(
                        column.getName(),
                        HoodieSchema.createNullable(fieldSchema),
                        column.getComment().orElse(null),
                        HoodieSchema.NULL_VALUE));
            }
            else {
                fields.add(HoodieSchemaField.of(column.getName(), fieldSchema, column.getComment().orElse(null), null));
            }
        }
        HoodieSchema record = HoodieSchema.createRecord(
                sanitizeName(tableName), NAMESPACE, null, fields.build());
        return HoodieSchemaUtils.addMetadataFields(record);
    }

    /**
     * Maps a single Trino type, failing with a message that names the type when no faithful Avro
     * representation exists. {@code path} identifies the column (and nested field, for a
     * {@code ROW}) so an error points at the offending column and so nested records get distinct
     * Avro names.
     */
    public static HoodieSchema toHoodieSchema(Type type, String path)
    {
        if (BOOLEAN.equals(type)) {
            return HoodieSchema.create(HoodieSchemaType.BOOLEAN);
        }
        // Avro's narrowest integer is int; both widen, and read back as INTEGER.
        if (TINYINT.equals(type) || SMALLINT.equals(type) || INTEGER.equals(type)) {
            return HoodieSchema.create(HoodieSchemaType.INT);
        }
        if (BIGINT.equals(type)) {
            return HoodieSchema.create(HoodieSchemaType.LONG);
        }
        if (REAL.equals(type)) {
            return HoodieSchema.create(HoodieSchemaType.FLOAT);
        }
        if (DOUBLE.equals(type)) {
            return HoodieSchema.create(HoodieSchemaType.DOUBLE);
        }
        if (DATE.equals(type)) {
            return HoodieSchema.createDate();
        }
        if (UUID.equals(type)) {
            throw unsupported(type, path, "the Hive Metastore has no UUID type, so the column could not be registered in the catalog; use VARCHAR");
        }
        if (type instanceof VarbinaryType) {
            return HoodieSchema.create(HoodieSchemaType.BYTES);
        }
        if (type instanceof DecimalType decimalType) {
            return HoodieSchema.createDecimal(decimalType.getPrecision(), decimalType.getScale());
        }
        if (type instanceof VarcharType) {
            // Any length bound is dropped; Avro strings are unbounded.
            return HoodieSchema.create(HoodieSchemaType.STRING);
        }
        if (type instanceof CharType) {
            throw unsupported(type, path, "Avro has no fixed-width string type, so CHAR padding semantics would be lost; use VARCHAR");
        }
        if (type instanceof TimeType) {
            throw unsupported(type, path, "the Hive Metastore has no TIME type, so the column could not be registered in the catalog");
        }
        if (type instanceof TimestampType timestampType) {
            int precision = timestampType.getPrecision();
            if (precision <= MAX_MILLIS_PRECISION) {
                return HoodieSchema.createTimestampMillis();
            }
            if (precision <= MAX_MICROS_PRECISION) {
                return HoodieSchema.createTimestampMicros();
            }
            throw unsupported(type, path, format(
                    "Avro timestamp logical types stop at microsecond precision; TIMESTAMP(%s) or narrower is supported",
                    MAX_MICROS_PRECISION));
        }
        if (type instanceof TimestampWithTimeZoneType) {
            throw unsupported(type, path, "Avro timestamp logical types carry an instant but no zone, so the per-value time zone would be lost; use TIMESTAMP without time zone");
        }
        if (type instanceof ArrayType arrayType) {
            return HoodieSchema.createArray(
                    HoodieSchema.createNullable(toHoodieSchema(arrayType.getElementType(), path + "_element")));
        }
        if (type instanceof MapType mapType) {
            if (!(mapType.getKeyType() instanceof VarcharType)) {
                throw unsupported(type, path, format(
                        "Avro map keys are always strings, but the key type is %s; use a VARCHAR key",
                        mapType.getKeyType().getDisplayName()));
            }
            return HoodieSchema.createMap(
                    HoodieSchema.createNullable(toHoodieSchema(mapType.getValueType(), path + "_value")));
        }
        if (type instanceof RowType rowType) {
            ImmutableList.Builder<HoodieSchemaField> fields = ImmutableList.builder();
            for (RowType.Field field : rowType.getFields()) {
                String fieldName = field.getName()
                        .orElseThrow(() -> unsupported(type, path, "Avro record fields must be named, but this ROW has an unnamed field"));
                fields.add(HoodieSchemaField.of(
                        fieldName,
                        HoodieSchema.createNullable(toHoodieSchema(field.getType(), path + "_" + fieldName)),
                        null,
                        HoodieSchema.NULL_VALUE));
            }
            return HoodieSchema.createRecord(sanitizeName(path), NAMESPACE, null, fields.build());
        }
        throw unsupported(type, path, "the Hudi connector has no Avro mapping for this type");
    }

    private static TrinoException unsupported(Type type, String path, String reason)
    {
        return new TrinoException(NOT_SUPPORTED, format(
                "Cannot create a Hudi table with column '%s' of type %s: %s",
                path, type.getDisplayName(), reason));
    }

    /**
     * Avro names must match {@code [A-Za-z_][A-Za-z0-9_]*}. Trino identifiers are already
     * lower-cased and permit characters Avro does not, so anything else becomes an underscore.
     */
    private static String sanitizeName(String name)
    {
        StringBuilder sanitized = new StringBuilder(name.length());
        for (int i = 0; i < name.length(); i++) {
            char character = name.charAt(i);
            boolean valid = character == '_'
                    || (character >= 'a' && character <= 'z')
                    || (character >= 'A' && character <= 'Z')
                    || (i > 0 && character >= '0' && character <= '9');
            sanitized.append(valid ? character : '_');
        }
        return sanitized.toString().toLowerCase(Locale.ROOT);
    }
}
