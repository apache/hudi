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
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
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
 * <p>
 * This is a stopgap for the Trino the module is pinned to. trinodb/trino#30545 moves the same check down into
 * {@code lib/trino-parquet}, where it can reconcile the two types rather than drop the domain - so it keeps
 * {@code float -> double} pruning - and it covers the bloom filter path too. Once {@code trino.sha} in the root pom
 * passes that commit, delete this class and its test instead of maintaining the two side by side.
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
     * Whether the parquet predicate can read a {@code fileType} column as {@code domainType} without misreading it,
     * which is the case only when the two describe the same physical values.
     * <p>
     * Two dispatch tables have to agree here, not one. {@code TupleDomainParquetPredicate.getDomain} picks its branch
     * on the domain type and reads the min/max statistics as that type; {@code checkInBloomFilter} picks a branch of
     * its own and hashes the looked-up value at the DOMAIN's width, while parquet-mr hashed the column at the FILE's.
     * A pair the first reads happily can therefore still make the second miss every lookup, and a bloom miss drops
     * the row group outright - that loses rows rather than merely failing to prune them. A pair is kept only when
     * each of the two either reads the file's own representation or declines to use it.
     * <p>
     * Dropping is not free either, so nothing is dropped without cause. {@code getDomain} answers a null-count
     * predicate before it dispatches on type at all - all-null statistics become {@code Domain.onlyNull} and a zero
     * null count becomes a not-null domain - so a dropped domain also forfeits {@code IS NULL} / {@code IS NOT NULL}
     * pruning, and a dropped {@code VARBINARY} or {@code UUID} domain forfeits bloom pruning that would have worked.
     * That is why the types {@code getDomain} has no branch for are kept rather than rejected: its fallthrough
     * returns a domain covering every value, which is safe whatever the file holds, and the null count and the bloom
     * filter still pay for themselves.
     * <p>
     * {@code TestParquetStatisticsDomains} pins every pair below against the real {@code getDomain}. The bloom half
     * leaves no domain to inspect, so it is pinned where the harm shows up instead, on the rows a real read returns:
     * {@code TestHudiEvolvedColumnPredicates.testEqualityOnAnIntColumnEvolvedToBigintStillFindsItsRows}.
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
            // asLong takes any integral box the statistics can hold, so the min/max side would read INT32 and INT64
            // interchangeably. checkInBloomFilter would not: it hashes a bigint lookup at eight bytes and every
            // narrower integral one at four, while parquet-mr hashed the column at its own width. A bigint over INT32
            // then finds nothing in the filter and the row group is dropped, so require the same width.
            // A decimal column reports its UNSCALED value though, which is the integer it stands for only at scale 0.
            return (BIGINT.equals(domainType) ? primitiveType == INT64 : primitiveType == INT32) && !isScaledDecimal(annotation);
        }
        if (domainType instanceof DecimalType domainDecimal) {
            // getShortDecimal and getLongDecimal rescale against the column's own annotation. Without one they read
            // the raw value as an unscaled decimal at the DOMAIN's scale, so an int or a string that evolved into a
            // decimal would be compared a factor of ten-to-the-scale off, or as raw UTF-8 bytes. The rescale itself
            // throws NUMERIC_VALUE_OUT_OF_RANGE once a file bound no longer fits the domain at the new scale, which
            // the predicate reports as the very corrupt-statistics failure this class exists to prevent. So keep
            // exactly Hudi's own lossless widening, which cannot overflow: neither the integer digits nor the scale
            // may shrink.
            return isDecimalPrimitive(primitiveType) && annotation instanceof DecimalLogicalTypeAnnotation fileDecimal
                    && fileDecimal.getPrecision() - fileDecimal.getScale() <= domainDecimal.getPrecision() - domainDecimal.getScale()
                    && fileDecimal.getScale() <= domainDecimal.getScale();
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
        if (domainType instanceof CharType || domainType instanceof VarbinaryType || domainType instanceof UuidType) {
            // getDomain has no branch for any of these, so it falls through to a domain covering every value and only
            // the null count prunes. checkInBloomFilter does have one for varbinary and uuid, hashing the lookup as
            // raw bytes, so the column still has to be one parquet-mr hashed as bytes.
            return primitiveType == BINARY || primitiveType == FIXED_LEN_BYTE_ARRAY;
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
