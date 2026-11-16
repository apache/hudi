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

import io.trino.spi.TrinoException;
import io.trino.spi.type.ArrayType;
import io.trino.spi.type.MapType;
import io.trino.spi.type.RowType;
import io.trino.spi.type.Type;
import io.trino.spi.type.TypeOperators;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.schema.HoodieSchema.TimePrecision;
import org.apache.hudi.common.schema.HoodieSchemaType;

import static io.trino.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DecimalType.createDecimalType;
import static io.trino.spi.type.Decimals.MAX_PRECISION;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimeType.createTimeType;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.createTimestampWithTimeZoneType;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.util.Objects.requireNonNull;

/**
 * Maps {@link HoodieSchema} to Trino types directly, one Trino type per {@link HoodieSchemaType}.
 * <p>
 * The connector does not use this mapping yet: its column types still come from the Hive type strings in the
 * metastore. That path gives different types for the rows marked below, because it goes through what Hive can
 * declare. Moving the connector onto this mapping changes those column types and is a separate, gated step.
 * <table>
 * <caption>HoodieSchema to Trino</caption>
 * <tr><th>HoodieSchema</th><th>Trino</th><th>Type identity</th></tr>
 * <tr><td>BOOLEAN, INT, LONG, FLOAT, DOUBLE</td><td>boolean, integer, bigint, real, double</td><td>kept</td></tr>
 * <tr><td>STRING, BYTES</td><td>varchar, varbinary</td><td>kept</td></tr>
 * <tr><td>DECIMAL(p, s)</td><td>decimal(p, s), up to precision 38</td><td>kept</td></tr>
 * <tr><td>DATE</td><td>date</td><td>kept</td></tr>
 * <tr><td>TIME millis, micros</td><td>time(3), time(6)</td><td>kept; integer or bigint through the metastore</td></tr>
 * <tr><td>TIMESTAMP millis, micros, UTC-adjusted</td><td>timestamp(3), timestamp(6) with time zone</td>
 * <td>kept; timestamp(9) through the metastore</td></tr>
 * <tr><td>TIMESTAMP millis, micros, local</td><td>timestamp(3), timestamp(6)</td><td>kept; timestamp(9) through the metastore</td></tr>
 * <tr><td>UUID</td><td>uuid</td><td>kept; varbinary through the metastore</td></tr>
 * <tr><td>ARRAY, MAP, RECORD</td><td>array, map with varchar keys, row</td><td>kept</td></tr>
 * <tr><td>ENUM</td><td>varchar</td><td>lost: Trino has no enum type</td></tr>
 * <tr><td>FIXED</td><td>varbinary</td><td>lost: Trino has no fixed-length binary type</td></tr>
 * <tr><td>VECTOR</td><td>array of real, double or tinyint</td><td>lost: the dimension; varbinary through the metastore</td></tr>
 * <tr><td>BLOB, VARIANT</td><td>row of their fields</td><td>lost: Trino sees a plain row</td></tr>
 * </table>
 * A nullable type maps as its non-null type; Trino carries nullability on the column. Other unions and NULL have no
 * Trino type and are rejected.
 */
public final class TrinoTypeMapper
{
    private final TypeOperators typeOperators;

    public TrinoTypeMapper(TypeOperators typeOperators)
    {
        this.typeOperators = requireNonNull(typeOperators, "typeOperators is null");
    }

    public Type toTypeSystem(HoodieSchema schema)
    {
        HoodieSchema type = schema.isNullable() ? schema.getNonNullType() : schema;
        return switch (type.getType()) {
            case BOOLEAN -> BOOLEAN;
            case INT -> INTEGER;
            case LONG -> BIGINT;
            case FLOAT -> REAL;
            case DOUBLE -> DOUBLE;
            case STRING, ENUM -> VARCHAR;
            case BYTES, FIXED -> VARBINARY;
            case UUID -> UUID;
            case DATE -> DATE;
            case DECIMAL -> {
                HoodieSchema.Decimal decimal = (HoodieSchema.Decimal) type;
                if (decimal.getPrecision() > MAX_PRECISION) {
                    throw unsupported(type, "Trino decimals stop at precision " + MAX_PRECISION);
                }
                yield createDecimalType(decimal.getPrecision(), decimal.getScale());
            }
            case TIME -> createTimeType(trinoPrecision(((HoodieSchema.Time) type).getPrecision()));
            case TIMESTAMP -> {
                HoodieSchema.Timestamp timestamp = (HoodieSchema.Timestamp) type;
                int precision = trinoPrecision(timestamp.getPrecision());
                // A UTC-adjusted timestamp is an instant; a local one is a wall-clock time
                yield timestamp.isUtcAdjusted() ? createTimestampWithTimeZoneType(precision) : createTimestampType(precision);
            }
            case ARRAY -> new ArrayType(toTypeSystem(type.getElementType()));
            case MAP -> new MapType(VARCHAR, toTypeSystem(type.getValueType()), typeOperators);
            case RECORD, BLOB, VARIANT -> RowType.from(type.getFields().stream()
                    .map(field -> RowType.field(field.name(), toTypeSystem(field.schema())))
                    .toList());
            case VECTOR -> new ArrayType(switch (((HoodieSchema.Vector) type).getVectorElementType()) {
                case FLOAT -> REAL;
                case DOUBLE -> DOUBLE;
                case INT8 -> TINYINT;
            });
            case UNION -> throw unsupported(type, "Trino has no union type");
            case NULL -> throw unsupported(type, "Trino has no null type");
        };
    }

    private static int trinoPrecision(TimePrecision precision)
    {
        return switch (precision) {
            case MILLIS -> 3;
            case MICROS -> 6;
        };
    }

    private static TrinoException unsupported(HoodieSchema schema, String reason)
    {
        return new TrinoException(NOT_SUPPORTED, "No Trino type for Hudi type %s (%s): %s".formatted(schema.getType(), schema, reason));
    }
}
