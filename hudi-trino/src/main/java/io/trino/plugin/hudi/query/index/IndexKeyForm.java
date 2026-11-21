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
package io.trino.plugin.hudi.query.index;

import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer;
import io.trino.plugin.hudi.util.HoodieSchemaAvroSerializer.ValueToAvro;
import io.trino.plugin.hudi.util.TrinoTypeMapper;
import io.trino.spi.block.Block;
import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.avro.Schema;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchema.TimePrecision;

import java.util.Optional;
import java.util.function.Function;

import static io.trino.spi.type.TypeUtils.writeNativeValue;
import static org.apache.hudi.common.avro.HoodieAvroUtils.convertValueForAvroLogicalTypes;
import static org.apache.hudi.common.util.StringUtils.objToString;

/**
 * The text an index keys a column value by, built from the value's Trino form through the Avro value Hudi stores for
 * the field ({@link HoodieSchemaAvroSerializer#toAvro}), so a filter literal looks up the key the writer wrote.
 */
public enum IndexKeyForm
{
    /**
     * A record key, as the Avro key generators write it ({@code KeyGenUtils#getRecordKey}): the stored value through
     * {@link org.apache.hudi.common.avro.HoodieAvroUtils#convertValueForAvroLogicalTypes}, then its string form. A date
     * keys as yyyy-MM-dd, a timestamp as a count of its own unit from the epoch, a time as its stored int or long, a
     * decimal as {@link java.math.BigDecimal#toString} at the field's scale.
     * <p>
     * {@code hoodie.datasource.write.keygenerator.consistent.logical.timestamp.enabled} is a writer option the table
     * config does not record, so its default, off, is assumed. A key written with it on (a UTC timestamp as
     * {@link java.sql.Timestamp} text) never equals a number, so such a lookup finds nothing and prunes nothing.
     */
    RECORD_KEY,
    /**
     * A secondary key, as {@code SecondaryIndexRecordGenerationUtils} writes it: the string form of the value the
     * engine's reader context returns, which for the types Hudi allows a secondary index on is the stored Avro value
     * (a date as its epoch day). A timestamp-millis is left out: Spark's reader context returns it in micros, Hudi's
     * Avro one in millis, so the key depends on which wrote the index entry.
     */
    SECONDARY_KEY;

    private static final TrinoTypeMapper TYPE_MAPPER = new TrinoTypeMapper(new TypeOperators());

    /**
     * Maps a Trino value of a column to the text this index keys it by, or is empty when there is no such text: the
     * field's type has no key form here, or the column does not carry the type {@link TrinoTypeMapper} gives the field
     * (a partition column keeps its metastore type), so its values do not convert to what the writer stored.
     *
     * @param fieldName the field, only to label conversion errors
     * @param fieldSchema the field's schema in the table schema
     * @param columnType the Trino type of the column's domain values
     */
    public Optional<Function<Object, String>> keyText(String fieldName, HoodieSchema fieldSchema, Type columnType)
    {
        if (fieldSchema.isComplexUnion()) {
            return Optional.empty();
        }
        HoodieSchema schema = fieldSchema.getNonNullType();
        if (!hasKeyText(schema) || !columnType.equals(TYPE_MAPPER.toTypeSystem(schema))) {
            return Optional.empty();
        }
        ValueToAvro toAvro = HoodieSchemaAvroSerializer.toAvro(fieldName, schema, columnType);
        Schema avroSchema = schema.toAvroSchema();
        return Optional.of(value -> {
            Object avroValue = toAvro.convert(singleValueBlock(columnType, value), 0);
            return switch (this) {
                case RECORD_KEY -> objToString(convertValueForAvroLogicalTypes(avroSchema, avroValue, false));
                case SECONDARY_KEY -> avroValue.toString();
            };
        });
    }

    private static Block singleValueBlock(Type type, Object value)
    {
        BlockBuilder builder = type.createBlockBuilder(null, 1);
        writeNativeValue(type, builder, value);
        return builder.build();
    }

    private boolean hasKeyText(HoodieSchema schema)
    {
        return switch (this) {
            case RECORD_KEY -> switch (schema.getType()) {
                case BOOLEAN, INT, LONG, FLOAT, DOUBLE, STRING, ENUM, BYTES, FIXED, UUID, DECIMAL, DATE, TIME, TIMESTAMP -> true;
                // A nested value's string form is not a key anyone writes
                case ARRAY, MAP, RECORD, BLOB, VARIANT, VECTOR, UNION, NULL -> false;
            };
            // The types HoodieIndexUtils.isSecondaryIndexSupportedType allows
            case SECONDARY_KEY -> switch (schema.getType()) {
                case INT, LONG, FLOAT, DOUBLE, STRING, DATE, TIME -> true;
                case TIMESTAMP -> ((HoodieSchema.Timestamp) schema).isUtcAdjusted()
                        && ((HoodieSchema.Timestamp) schema).getPrecision() == TimePrecision.MICROS;
                case BOOLEAN, ENUM, BYTES, FIXED, UUID, DECIMAL, ARRAY, MAP, RECORD, BLOB, VARIANT, VECTOR, UNION, NULL -> false;
            };
        };
    }
}
