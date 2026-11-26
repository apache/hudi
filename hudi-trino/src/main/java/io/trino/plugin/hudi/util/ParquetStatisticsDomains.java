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
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.LongTimestampWithTimeZone;
import io.trino.spi.type.TimestampType;
import io.trino.spi.type.TimestampWithTimeZoneType;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.Timestamps.MICROSECONDS_PER_MILLISECOND;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MICROSECOND;
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
        return dropIncomparableDomains(parquetTupleDomain, Set.of());
    }

    /**
     * {@link #dropIncomparableDomains(TupleDomain)}, also dropping the domain of every column in
     * {@code misstatedTimestampPaths}, the file paths of the columns whose annotation misstates the timestamp unit their
     * field declares ({@link ParquetReadConversions#storedTimestampUnit}): their statistics read in the wrong unit, a
     * thousand times off, and would drop row groups holding matching rows. The caller resolves those paths from the same
     * column handles the read repairs, so the two agree on which file column a field is whether columns are matched by
     * name or by position.
     */
    public static TupleDomain<ColumnDescriptor> dropIncomparableDomains(TupleDomain<ColumnDescriptor> parquetTupleDomain, Set<List<String>> misstatedTimestampPaths)
    {
        if (parquetTupleDomain.isAll() || parquetTupleDomain.isNone()) {
            return parquetTupleDomain;
        }

        Map<ColumnDescriptor, Domain> domains = parquetTupleDomain.getDomains().orElseThrow();
        Map<ColumnDescriptor, Domain> comparableDomains = new LinkedHashMap<>();
        boolean changed = false;
        for (Map.Entry<ColumnDescriptor, Domain> entry : domains.entrySet()) {
            Domain domain = entry.getValue();
            PrimitiveType fileType = entry.getKey().getPrimitiveType();
            if (misstatedTimestampPaths.contains(List.of(entry.getKey().getPath()))) {
                log.debug("Not pushing down a %s predicate on %s: the file misstates the column's timestamp unit", domain.getType(), entry.getKey());
                changed = true;
                continue;
            }
            Optional<Domain> utcTimestampDomain = asUtcTimestampDomain(domain, fileType);
            if (utcTimestampDomain.isPresent()) {
                comparableDomains.put(entry.getKey(), utcTimestampDomain.get());
                changed = true;
            }
            else if (hasComparableStatistics(domain.getType(), fileType)) {
                comparableDomains.put(entry.getKey(), domain);
            }
            else {
                log.debug("Not pushing down a %s predicate on %s: the file stores it as %s, so the column statistics cannot answer it",
                        domain.getType(), entry.getKey(), fileType);
                changed = true;
            }
        }
        if (!changed) {
            return parquetTupleDomain;
        }
        return TupleDomain.withColumnDomains(comparableDomains);
    }

    /**
     * A {@code timestamp with time zone} domain rewritten as the {@code timestamp} domain of the same UTC instants,
     * for an INT64 column the file annotates as UTC-adjusted, or an INT96 column above millisecond precision; empty for
     * every other domain or column.
     * <p>
     * {@code TupleDomainParquetPredicate.getDomain} has no branch for {@code timestamp with time zone}, so such a
     * domain would only ever prune on the null count. Its {@code timestamp} branch decodes a UTC-adjusted column into
     * the predicate's time zone, which {@code HudiPageSourceProvider} fixes at UTC: so a UTC-adjusted column's
     * statistics read as the wall-clock times of its instants at UTC, which is what each instant maps to here. INT96, an
     * instant too, decodes into the same zone (and has usable statistics only where a row group's minimum equals its
     * maximum, PARQUET-1065, or from its dictionary), but only above millisecond precision: the statistics side rounds
     * sub-millisecond nanos ({@code ShortTimestampEncoder}) while the reader truncates them into a short timestamp
     * with time zone, so at millisecond precision a stored .000600 would read .000 but prune as .001. Above it both
     * sides round. The map
     * is exact and order-preserving at Hudi's precisions (millis, micros), so every range keeps its bounds and
     * inclusiveness.
     */
    static Optional<Domain> asUtcTimestampDomain(Domain domain, PrimitiveType fileType)
    {
        if (!(domain.getType() instanceof TimestampWithTimeZoneType withTimeZoneType)
                || withTimeZoneType.getPrecision() > TimestampType.MAX_SHORT_PRECISION
                || !(isUtcAdjustedInt64Timestamp(fileType) || (fileType.getPrimitiveTypeName() == INT96 && !withTimeZoneType.isShort()))) {
            return Optional.empty();
        }
        TimestampType timestampType = createTimestampType(withTimeZoneType.getPrecision());
        ValueSet values = domain.getValues().getValuesProcessor().transform(
                ranges -> {
                    List<Range> utcRanges = new ArrayList<>(ranges.getRangeCount());
                    for (Range range : ranges.getOrderedRanges()) {
                        utcRanges.add(utcRange(
                                timestampType,
                                range.getLowValue().map(value -> toEpochMicros(withTimeZoneType, value)),
                                range.isLowInclusive(),
                                range.getHighValue().map(value -> toEpochMicros(withTimeZoneType, value)),
                                range.isHighInclusive()));
                    }
                    return ValueSet.ofRanges(utcRanges);
                },
                _ -> {
                    throw new IllegalStateException("A timestamp with time zone domain is a range set");
                },
                allOrNone -> allOrNone.isAll() ? ValueSet.all(timestampType) : ValueSet.none(timestampType));
        return Optional.of(Domain.create(values, domain.isNullAllowed()));
    }

    private static boolean isUtcAdjustedInt64Timestamp(PrimitiveType fileType)
    {
        return fileType.getPrimitiveTypeName() == INT64
                && fileType.getLogicalTypeAnnotation() instanceof TimestampLogicalTypeAnnotation annotation
                && annotation.isAdjustedToUTC()
                && annotation.getUnit() != null;
    }

    private static Range utcRange(TimestampType type, Optional<Long> low, boolean lowInclusive, Optional<Long> high, boolean highInclusive)
    {
        if (low.isPresent() && high.isPresent()) {
            return Range.range(type, low.get(), lowInclusive, high.get(), highInclusive);
        }
        if (low.isPresent()) {
            return lowInclusive ? Range.greaterThanOrEqual(type, low.get()) : Range.greaterThan(type, low.get());
        }
        if (high.isPresent()) {
            return highInclusive ? Range.lessThanOrEqual(type, high.get()) : Range.lessThan(type, high.get());
        }
        return Range.all(type);
    }

    private static long toEpochMicros(TimestampWithTimeZoneType type, Object value)
    {
        if (type.isShort()) {
            return unpackMillisUtc((long) value) * MICROSECONDS_PER_MILLISECOND;
        }
        LongTimestampWithTimeZone timestamp = (LongTimestampWithTimeZone) value;
        return timestamp.getEpochMillis() * MICROSECONDS_PER_MILLISECOND + timestamp.getPicosOfMilli() / PICOSECONDS_PER_MICROSECOND;
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
        if (domainType instanceof CharType || domainType instanceof VarbinaryType) {
            // getDomain has no branch for either, so it falls through to a domain covering every value and only
            // the null count prunes. checkInBloomFilter does have one for varbinary, hashing the lookup as
            // raw bytes, so the column still has to be one parquet-mr hashed as bytes.
            return primitiveType == BINARY || primitiveType == FIXED_LEN_BYTE_ARRAY;
        }
        if (domainType instanceof UuidType) {
            // As for varbinary, but checkInBloomFilter hashes the uuid's 16 raw bytes, which matches only a
            // FIXED_LEN_BYTE_ARRAY uuid. Hudi writes a uuid as its 36-character text by default, and a bloom miss on
            // that column would drop the row group with its rows.
            return primitiveType == FIXED_LEN_BYTE_ARRAY;
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
