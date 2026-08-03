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

import io.airlift.log.Logger;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimestampLogicalTypeAnnotation;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;

/**
 * Keeps a pushed-down predicate from being matched against statistics it cannot be compared with.
 * <p>
 * {@code TupleDomainParquetPredicate.getDomain} selects its branch on the type of the pushed-down DOMAIN and then
 * reads the parquet statistics as that type - {@code Double min = (Double) minimums.get(i)} and so on. The domain's
 * type comes from the metastore while the statistics come from the file, and Hudi's type evolution is exactly what
 * makes those two disagree: after a column evolves and the metastore is synced, every base file written before the
 * evolution still stores the old physical type. Handing such a domain to the parquet predicate either fails the
 * whole split with {@code Malformed Parquet file. Corrupted statistics for column ...} wrapping a
 * {@link ClassCastException}, or - where the two types happen to share a representation, as a decimal and a varchar
 * both do through {@code Slice} - silently prunes row groups on a comparison that means nothing.
 * <p>
 * The read path has no such problem: {@code ColumnReaderFactory} decodes parquet {@code FLOAT} into Trino
 * {@code DOUBLE} and {@code INT32} into {@code BIGINT} natively, and {@code ParquetTypeTranslator.createCoercer}
 * covers the rest of the promotions the page source is asked for. Only the statistics side is blind, so only the
 * statistics side needs the guard.
 */
public final class ParquetStatisticsDomains
{
    private static final Logger log = Logger.get(ParquetStatisticsDomains.class);

    private ParquetStatisticsDomains() {}

    /**
     * Drops every domain whose type cannot be compared against its column's statistics, keeping the rest untouched.
     * <p>
     * Dropping loses row group pruning for that column but never a row: {@code HudiMetadata.applyFilter} hands the
     * whole regular predicate back to the engine as the remaining filter and does not precalculate statistics for
     * the pushdown, so a connector-side domain is an optimization and nothing else. The dynamic half of the
     * predicate is redundant with the join above the scan by construction. It is the same trade
     * {@code HudiPageSourceProvider.remapPredicateColumnIndicesToPhysical} already makes for a predicate column the
     * file does not carry, and the same one {@code HudiColumnStatsIndexSupport.getDomainFromColumnStats} makes when
     * the metadata table's column statistics do not match the column's type.
     * <p>
     * The filter runs on the descriptor-keyed domain rather than on the column handles it was built from. That is
     * what the parquet predicate itself will be evaluated against, so the check and the evaluation cannot disagree
     * about which column or which physical type is meant; it covers both values of
     * {@code hudi.parquet.use-column-names} in one pass, since a handle is resolved to a descriptor before either;
     * and a dereference handle contributes the leaf field's type without any extra work.
     */
    public static TupleDomain<ColumnDescriptor> dropIncomparableDomains(TupleDomain<ColumnDescriptor> parquetTupleDomain)
    {
        if (parquetTupleDomain.isAll() || parquetTupleDomain.isNone()) {
            return parquetTupleDomain;
        }

        Map<ColumnDescriptor, Domain> domains = parquetTupleDomain.getDomains().orElseThrow();
        Map<ColumnDescriptor, Domain> comparableDomains = new LinkedHashMap<>();
        for (Map.Entry<ColumnDescriptor, Domain> entry : domains.entrySet()) {
            if (hasComparableStatistics(entry.getValue().getType(), entry.getKey().getPrimitiveType())) {
                comparableDomains.put(entry.getKey(), entry.getValue());
            }
            else {
                log.debug("Not pushing down a %s predicate on %s: the file stores it as %s, so the column statistics cannot answer it",
                        entry.getValue().getType(), entry.getKey(), entry.getKey().getPrimitiveType());
            }
        }
        if (comparableDomains.size() == domains.size()) {
            return parquetTupleDomain;
        }
        return TupleDomain.withColumnDomains(comparableDomains);
    }

    /**
     * Whether {@code TupleDomainParquetPredicate.getDomain} builds a meaningful domain out of a {@code fileType}
     * column's statistics when asked for {@code domainType}, which is the case only when the two describe the same
     * physical values.
     * <p>
     * The accepted pairs mirror that method's dispatch branch for branch. Everything else is rejected, which for a
     * type it has no branch for - {@code CHAR}, {@code VARBINARY}, {@code UUID}, {@code TIME}, a timestamp with time
     * zone - costs nothing at all: its fallthrough returns a domain covering every value, which prunes exactly as
     * much as pushing nothing down. Only the accepted pairs can be wrong, and
     * {@code TestParquetStatisticsDomains} pins each of them against the real {@code getDomain}.
     */
    public static boolean hasComparableStatistics(Type domainType, PrimitiveType fileType)
    {
        PrimitiveTypeName primitiveType = fileType.getPrimitiveTypeName();
        LogicalTypeAnnotation annotation = fileType.getLogicalTypeAnnotation();

        if (BOOLEAN.equals(domainType)) {
            return primitiveType == PrimitiveTypeName.BOOLEAN;
        }
        if (TINYINT.equals(domainType) || SMALLINT.equals(domainType) || INTEGER.equals(domainType)
                || BIGINT.equals(domainType) || DATE.equals(domainType)) {
            // asLong takes any integral box the statistics can hold, so INT32 and INT64 are interchangeable here.
            // A decimal column reports its UNSCALED value though, which is the integer it stands for only at scale 0.
            return (primitiveType == INT32 || primitiveType == INT64) && !isScaledDecimal(annotation);
        }
        if (domainType instanceof DecimalType) {
            // getShortDecimal and getLongDecimal rescale against the column's own annotation. Without one they read
            // the raw value as an unscaled decimal at the DOMAIN's scale, so an int or a string that evolved into a
            // decimal would be compared a factor of ten-to-the-scale off, or as raw UTF-8 bytes.
            return isDecimalPrimitive(primitiveType) && annotation instanceof DecimalLogicalTypeAnnotation;
        }
        if (REAL.equals(domainType)) {
            return primitiveType == FLOAT;
        }
        if (DOUBLE.equals(domainType)) {
            return primitiveType == PrimitiveTypeName.DOUBLE;
        }
        if (domainType instanceof VarcharType) {
            // Both sides compare raw bytes, which is varchar's own ordering - unless the bytes are a decimal's
            // big-endian two's complement, which orders nothing like the digits it prints as.
            return (primitiveType == BINARY || primitiveType == FIXED_LEN_BYTE_ARRAY)
                    && !(annotation instanceof DecimalLogicalTypeAnnotation);
        }
        if (domainType instanceof TimestampType) {
            // INT96 is read from the binary statistics and INT64 from the long ones, but an INT64 column has to say
            // which unit it counts in before its bounds mean anything.
            return primitiveType == INT96
                    || (primitiveType == INT64 && annotation instanceof TimestampLogicalTypeAnnotation timestampAnnotation && timestampAnnotation.getUnit() != null);
        }
        return false;
    }

    private static boolean isDecimalPrimitive(PrimitiveTypeName primitiveType)
    {
        return primitiveType == INT32 || primitiveType == INT64 || primitiveType == BINARY || primitiveType == FIXED_LEN_BYTE_ARRAY;
    }

    private static boolean isScaledDecimal(LogicalTypeAnnotation annotation)
    {
        return annotation instanceof DecimalLogicalTypeAnnotation decimalAnnotation && decimalAnnotation.getScale() != 0;
    }
}
