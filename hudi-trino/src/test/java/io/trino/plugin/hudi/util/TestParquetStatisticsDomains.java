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

import io.airlift.slice.Slices;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.CharType;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Int128;
import io.trino.spi.type.Type;
import io.trino.spi.type.UuidType;
import io.trino.spi.type.VarbinaryType;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.DecimalLogicalTypeAnnotation;
import org.apache.parquet.schema.LogicalTypeAnnotation.TimeUnit;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hudi.util.ParquetStatisticsDomains.dropIncomparableDomains;
import static io.trino.plugin.hudi.util.ParquetStatisticsDomains.hasComparableStatistics;
import static io.trino.plugin.hudi.util.TestParquetStatisticsDomains.LibraryOutcome.ALL;
import static io.trino.plugin.hudi.util.TestParquetStatisticsDomains.LibraryOutcome.NARROW;
import static io.trino.plugin.hudi.util.TestParquetStatisticsDomains.LibraryOutcome.THROWS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.CharType.createCharType;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
import static io.trino.spi.type.UuidType.UUID;
import static io.trino.spi.type.VarbinaryType.VARBINARY;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.FLOAT;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT96;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Pins {@link ParquetStatisticsDomains#hasComparableStatistics} against the two methods it exists to protect. Every
 * case below states BOTH what the guard decides and what the library actually does with the same pair, and the check
 * is run against the real {@code TupleDomainParquetPredicate}, not a description of it. A Trino upgrade that moves a
 * branch therefore fails here, where the mismatch is a line of test output, instead of in a query.
 * <p>
 * The three library outcomes are worth telling apart, because the guard exists for more than one reason:
 * <ul>
 *     <li>{@link LibraryOutcome#THROWS} - the cast fails and the whole split dies with {@code HUDI_BAD_DATA}. This
 *     is apache/hudi#19457 as reported.</li>
 *     <li>{@link LibraryOutcome#NARROW} on a pair the guard drops - far worse: a domain IS produced, from bytes that
 *     mean something else entirely, and row groups get pruned on a comparison that is simply false. Nothing fails,
 *     rows just go missing.</li>
 *     <li>{@link LibraryOutcome#ALL} - {@code getDomain} declines the pair itself. Keeping such a pair is still
 *     worth it when the bloom filter can answer it, and safe either way, because the declined domain covers every
 *     value and only the column's null count prunes.</li>
 * </ul>
 * A kept pair must never be one the library reads out of a representation that means something else, so every
 * cross-type kept pair states the exact domain {@code getDomain} has to come back with. Asserting only that the
 * result is narrower than "any value" would pass a rescale that silently landed a factor of a hundred out.
 * <p>
 * {@code getDomain} is only half of what the guard has to mirror. The other half is {@code checkInBloomFilter},
 * which dispatches on the domain type of its own accord and hashes the lookup at that type's width - the reason the
 * integral arm insists on a matching physical width and the reason the byte-typed arm exists at all. Its bounds are
 * not visible in a domain, so it is pinned where the harm shows up instead, on the row groups a real read returns:
 * {@code TestHudiEvolvedColumnPredicates.testEqualityOnAnIntColumnEvolvedToBigintStillFindsItsRows}.
 */
class TestParquetStatisticsDomains
{
    private static final ParquetDataSourceId DATA_SOURCE_ID = new ParquetDataSourceId("test");
    private static final long VALUE_COUNT = 10;
    private static final int MIN_VALUE = 1;
    private static final int MAX_VALUE = 2;

    enum LibraryOutcome
    {
        /** getDomain read the statistics and returned a range narrower than "any value". */
        NARROW,
        /** getDomain declined to use the statistics and returned a domain covering every value. */
        ALL,
        /** getDomain failed, which the connector reports as a corrupt-statistics error over the whole split. */
        THROWS,
    }

    private record TypePair(String description, Type domainType, PrimitiveType fileType, boolean comparable,
                            LibraryOutcome outcome, Optional<Domain> expectedDomain)
    {
        @Override
        public String toString()
        {
            return description;
        }
    }

    private static List<TypePair> typePairs()
    {
        return List.of(
                // A column that never evolved: the domain's type is the one the file was written with
                kept("boolean over BOOLEAN", BOOLEAN, plain(PrimitiveTypeName.BOOLEAN), NARROW),
                kept("integer over INT32", INTEGER, plain(INT32), NARROW),
                kept("bigint over INT64", BIGINT, plain(INT64), NARROW),
                kept("tinyint over INT32", TINYINT, plain(INT32), NARROW),
                kept("date over INT32 date", DATE, annotated(INT32, LogicalTypeAnnotation.dateType()), NARROW),
                kept("real over FLOAT", REAL, plain(FLOAT), NARROW),
                kept("double over DOUBLE", DOUBLE, plain(PrimitiveTypeName.DOUBLE), NARROW),
                kept("varchar over BINARY string", VARCHAR, annotated(BINARY, LogicalTypeAnnotation.stringType()), NARROW),
                kept("decimal(9,2) over INT32 decimal(9,2)", DecimalType.createDecimalType(9, 2), decimal(INT32, 9, 2), NARROW),
                kept("timestamp over INT64 timestamp", TIMESTAMP_MILLIS, annotated(INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS)), NARROW),
                kept("timestamp over INT96", TIMESTAMP_MILLIS, plain(INT96), NARROW),
                // A char, a varbinary and a uuid all fall past every getDomain branch, so it declines them and only
                // the null count prunes. Keeping them costs nothing and buys back the bloom filter, which does have
                // a branch for the last two.
                kept("char over BINARY", createCharType(4), plain(BINARY), ALL),
                kept("varbinary over BINARY", VARBINARY, plain(BINARY), ALL),
                kept("uuid over FIXED_LEN_BYTE_ARRAY", UUID, plain(FIXED_LEN_BYTE_ARRAY), ALL),

                // Promotions the statistics can answer, so pushdown must survive them. The domain each one has to
                // come back with is spelled out: a rescale that lands a factor of ten out is still "narrow".
                keptReading("decimal(20,2) -> decimal(38,4)", DecimalType.createDecimalType(38, 4), decimal(FIXED_LEN_BYTE_ARRAY, 20, 2),
                        rangeOf(DecimalType.createDecimalType(38, 4), Int128.valueOf(100), Int128.valueOf(200))),
                keptReading("decimal(9,2) over BINARY decimal(9,2)", DecimalType.createDecimalType(9, 2), decimal(BINARY, 9, 2),
                        rangeOf(DecimalType.createDecimalType(9, 2), (long) MIN_VALUE, (long) MAX_VALUE)),
                keptReading("integer over a zero-scale INT32 decimal", INTEGER, decimal(INT32, 9, 0),
                        rangeOf(INTEGER, (long) MIN_VALUE, (long) MAX_VALUE)),
                // Every table synced with hive_sync.support_timestamp=false carries this pair: HiveSchemaUtil maps
                // TIMESTAMP to BIGINT, so the metastore says bigint while the file stays an annotated INT64.
                keptReading("bigint over INT64 timestamp(micros)", BIGINT, annotated(INT64, LogicalTypeAnnotation.timestampType(true, TimeUnit.MICROS)),
                        rangeOf(BIGINT, (long) MIN_VALUE, (long) MAX_VALUE)),
                keptReading("varchar over a plain FIXED_LEN_BYTE_ARRAY", VARCHAR, plain(FIXED_LEN_BYTE_ARRAY),
                        rangeOf(VARCHAR, sliceOf(plain(FIXED_LEN_BYTE_ARRAY), MIN_VALUE), sliceOf(plain(FIXED_LEN_BYTE_ARRAY), MAX_VALUE))),

                // Promotions the min/max side reads correctly but the BLOOM side does not. checkInBloomFilter hashes
                // the lookup at the domain's width, parquet-mr hashed the column at the file's, so the filter misses
                // and the row group is dropped with its rows still in it. See apache/hudi#19457 and
                // trinodb/trino#30544; testABigintLookupNeverFindsAnInt32ColumnsBloomHashes pins the mechanism.
                dropped("int -> long", BIGINT, plain(INT32), NARROW),
                dropped("long -> int", INTEGER, plain(INT64), NARROW),
                // Hudi rejects this evolution as lossy (HoodieSchemaCompatibilityChecker lets neither the integer
                // digits nor the scale shrink) and so does the guard: the rescale in getShortDecimal multiplies by a
                // hundred, which throws NUMERIC_VALUE_OUT_OF_RANGE for any file bound over 9999999 -- read as
                // "Corrupted statistics", the very failure this class removes. Small bounds hide that, hence the drop
                // is on the type pair rather than on the values.
                dropped("decimal(9,2) -> decimal(9,4), lossy", DecimalType.createDecimalType(9, 4), decimal(INT32, 9, 2), NARROW),

                // Promotions that fail the split today: apache/hudi#19457 and its neighbours
                dropped("float -> double", DOUBLE, plain(FLOAT), THROWS),
                dropped("int -> double", DOUBLE, plain(INT32), THROWS),
                dropped("long -> double", DOUBLE, plain(INT64), THROWS),
                dropped("int -> float", REAL, plain(INT32), THROWS),
                dropped("int -> string", VARCHAR, plain(INT32), THROWS),
                dropped("long -> string", VARCHAR, plain(INT64), THROWS),
                dropped("float -> string", VARCHAR, plain(FLOAT), THROWS),
                dropped("double -> string", VARCHAR, plain(PrimitiveTypeName.DOUBLE), THROWS),
                dropped("string -> date", DATE, annotated(BINARY, LogicalTypeAnnotation.stringType()), THROWS),

                // Promotions that silently prune on a comparison that means nothing, which is why the guard cannot
                // be a try/catch around the cast
                dropped("decimal -> string", VARCHAR, decimal(FIXED_LEN_BYTE_ARRAY, 20, 2), NARROW),
                dropped("string -> decimal", DecimalType.createDecimalType(9, 2), annotated(BINARY, LogicalTypeAnnotation.stringType()), NARROW),
                dropped("int -> decimal", DecimalType.createDecimalType(9, 2), plain(INT32), NARROW),
                dropped("integer over a scaled INT32 decimal", INTEGER, decimal(INT32, 9, 2), NARROW),

                // Pairs getDomain declines on its own. Dropping costs nothing on the min/max side, but a varbinary
                // over an INT32 column would still be hashed as raw bytes against a filter built from four-byte
                // integer hashes, so the guard has to drop it rather than lean on getDomain declining.
                dropped("timestamp over an unannotated INT64", TIMESTAMP_MILLIS, plain(INT64), ALL),
                dropped("varbinary over INT32", VARBINARY, plain(INT32), ALL));
    }

    @ParameterizedTest
    @MethodSource("typePairs")
    public void testGuardMatchesTheParquetPredicate(TypePair pair)
            throws Exception
    {
        assertThat(hasComparableStatistics(pair.domainType(), pair.fileType()))
                .as("guard verdict for %s", pair)
                .isEqualTo(pair.comparable());

        ColumnDescriptor descriptor = descriptorOf(pair.fileType());
        Statistics<?> statistics = statisticsOf(pair.fileType());
        if (pair.outcome() == THROWS) {
            assertThatThrownBy(() -> TupleDomainParquetPredicate.getDomain(descriptor, pair.domainType(), VALUE_COUNT, statistics, DATA_SOURCE_ID, DateTimeZone.UTC))
                    .as("getDomain for %s", pair)
                    .hasMessageContaining("Corrupted statistics");
            return;
        }

        Domain domain = TupleDomainParquetPredicate.getDomain(descriptor, pair.domainType(), VALUE_COUNT, statistics, DATA_SOURCE_ID, DateTimeZone.UTC);
        assertThat(domain.getValues().isAll())
                .as("getDomain for %s returned %s", pair, domain)
                .isEqualTo(pair.outcome() == ALL);
        pair.expectedDomain().ifPresent(expected -> assertThat(domain)
                .as("bounds getDomain read for %s", pair)
                .isEqualTo(expected));
    }

    @ParameterizedTest
    @MethodSource("typePairs")
    public void testNoKeptPairIsMisread(TypePair pair)
    {
        // The invariant behind the whole table: a false negative only costs pruning, but a false positive is either a
        // failed query or a wrong one. So nothing may be kept that the library cannot read -- either it produces a
        // real range out of the file's own bytes, or it declines the statistics and leaves the null count to prune.
        if (!hasComparableStatistics(pair.domainType(), pair.fileType())) {
            return;
        }
        assertThat(pair.outcome()).as("library outcome for the kept pair %s", pair).isIn(NARROW, ALL);
        // A pair getDomain declines is worth keeping only when something else pays for it: the bloom filter, which
        // has a branch for varbinary and uuid, and the null count, which every type gets. Keeping a declined pair
        // anywhere else would be a guard that mirrors getDomain's dispatch wrongly rather than deliberately.
        if (pair.outcome() == ALL) {
            assertThat(pair.domainType())
                    .as("declined pair %s is kept only for its bloom filter and null count", pair)
                    .isInstanceOfAny(CharType.class, VarbinaryType.class, UuidType.class);
        }
    }

    @Test
    public void testAllAndNonePassThroughUntouched()
    {
        assertThat(dropIncomparableDomains(TupleDomain.all())).isEqualTo(TupleDomain.all());
        assertThat(dropIncomparableDomains(TupleDomain.none())).isEqualTo(TupleDomain.none());
    }

    @Test
    public void testOnlyTheIncomparableDomainIsDropped()
    {
        // Distinct names on purpose: a ColumnDescriptor is keyed by its path, so two columns sharing one name would
        // collapse into a single map entry and the test would pass without ever exercising the filtering
        ColumnDescriptor evolved = descriptorNamed("evolved", FLOAT);
        ColumnDescriptor stable = descriptorNamed("stable", INT32);
        Domain doubleDomain = Domain.singleValue(DOUBLE, 1.0d);
        Domain intDomain = Domain.singleValue(INTEGER, 1L);

        TupleDomain<ColumnDescriptor> filtered = dropIncomparableDomains(
                TupleDomain.withColumnDomains(Map.of(evolved, doubleDomain, stable, intDomain)));

        assertThat(filtered.getDomains().orElseThrow()).containsExactly(Map.entry(stable, intDomain));
    }

    @Test
    public void testAPredicateWithNothingToDropIsReturnedAsIs()
    {
        TupleDomain<ColumnDescriptor> predicate = TupleDomain.withColumnDomains(
                Map.of(descriptorOf(plain(INT32)), Domain.singleValue(INTEGER, 1L)));

        assertThat(dropIncomparableDomains(predicate)).isSameAs(predicate);
    }

    @Test
    public void testDroppingEveryDomainLeavesAnUnconstrainedPredicate()
    {
        TupleDomain<ColumnDescriptor> predicate = TupleDomain.withColumnDomains(
                Map.of(descriptorOf(plain(FLOAT)), Domain.singleValue(DOUBLE, 1.0d)));

        // Not TupleDomain.none(): dropping means "do not prune on this", never "this matches nothing"
        assertThat(dropIncomparableDomains(predicate).isAll()).as("everything dropped").isTrue();
    }

    private static TypePair kept(String description, Type domainType, PrimitiveType fileType, LibraryOutcome outcome)
    {
        return new TypePair(description, domainType, fileType, true, outcome, Optional.empty());
    }

    /** A kept pair whose domain type is not the file's own, so the exact bounds it reads are worth pinning. */
    private static TypePair keptReading(String description, Type domainType, PrimitiveType fileType, Domain expectedDomain)
    {
        return new TypePair(description, domainType, fileType, true, NARROW, Optional.of(expectedDomain));
    }

    private static TypePair dropped(String description, Type domainType, PrimitiveType fileType, LibraryOutcome outcome)
    {
        return new TypePair(description, domainType, fileType, false, outcome, Optional.empty());
    }

    /** The statistics fixture holds {@link #MIN_VALUE} and {@link #MAX_VALUE}, never null, so nulls are not allowed. */
    private static Domain rangeOf(Type type, Object min, Object max)
    {
        return Domain.create(ValueSet.ofRanges(Range.range(type, min, true, max, true)), false);
    }

    private static PrimitiveType plain(PrimitiveTypeName primitiveTypeName)
    {
        if (primitiveTypeName == FIXED_LEN_BYTE_ARRAY) {
            return Types.primitive(primitiveTypeName, OPTIONAL).length(16).named("c");
        }
        return Types.primitive(primitiveTypeName, OPTIONAL).named("c");
    }

    private static PrimitiveType annotated(PrimitiveTypeName primitiveTypeName, LogicalTypeAnnotation annotation)
    {
        return Types.primitive(primitiveTypeName, OPTIONAL).as(annotation).named("c");
    }

    private static PrimitiveType decimal(PrimitiveTypeName primitiveTypeName, int precision, int scale)
    {
        Types.PrimitiveBuilder<PrimitiveType> builder = Types.primitive(primitiveTypeName, OPTIONAL);
        if (primitiveTypeName == FIXED_LEN_BYTE_ARRAY) {
            builder = builder.length(16);
        }
        return builder.as(LogicalTypeAnnotation.decimalType(scale, precision)).named("c");
    }

    private static ColumnDescriptor descriptorOf(PrimitiveType fileType)
    {
        return new ColumnDescriptor(new String[] {fileType.getName()}, fileType, 0, 1);
    }

    private static ColumnDescriptor descriptorNamed(String name, PrimitiveTypeName primitiveTypeName)
    {
        return descriptorOf(Types.primitive(primitiveTypeName, OPTIONAL).named(name));
    }

    private static io.airlift.slice.Slice sliceOf(PrimitiveType fileType, int value)
    {
        return Slices.wrappedBuffer(statisticsBytes(fileType, value));
    }

    /**
     * Statistics holding a small, non-degenerate range, so that a pair the library CAN read produces a domain
     * narrower than "any value" and the {@link LibraryOutcome#NARROW} cases stay distinguishable from
     * {@link LibraryOutcome#ALL}. Two nearby values also keep every integral type clear of
     * {@code isStatisticsOverflow}, which would otherwise widen tinyint back to everything.
     */
    private static Statistics<?> statisticsOf(PrimitiveType fileType)
    {
        return Statistics.getBuilderForReading(fileType)
                .withMin(statisticsBytes(fileType, MIN_VALUE))
                .withMax(statisticsBytes(fileType, MAX_VALUE))
                .withNumNulls(0)
                .build();
    }

    private static byte[] statisticsBytes(PrimitiveType fileType, int value)
    {
        return switch (fileType.getPrimitiveTypeName()) {
            // Both bounds false, so the boolean branch reports "only false" rather than "true and false", which it
            // would report as every value
            case BOOLEAN -> new byte[] {0};
            case INT32 -> ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putInt(value).array();
            case INT64 -> ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putLong(value).array();
            case FLOAT -> ByteBuffer.allocate(4).order(LITTLE_ENDIAN).putFloat(value).array();
            case DOUBLE -> ByteBuffer.allocate(8).order(LITTLE_ENDIAN).putDouble(value).array();
            // INT96 statistics are only usable when the bounds are equal (PARQUET-1065), so ignore the value:
            // 8 bytes of nanos-within-the-day followed by the julian day of the epoch
            case INT96 -> ByteBuffer.allocate(12).order(LITTLE_ENDIAN).putLong(0).putInt(2440588).array();
            // A decimal's unscaled value is big-endian two's complement whichever physical type carries it, so a
            // BINARY decimal cannot reuse the digits a BINARY string is written as
            case BINARY -> fileType.getLogicalTypeAnnotation() instanceof DecimalLogicalTypeAnnotation
                    ? new byte[] {(byte) value}
                    : Integer.toString(value).getBytes(UTF_8);
            case FIXED_LEN_BYTE_ARRAY -> ByteBuffer.allocate(fileType.getTypeLength()).put(fileType.getTypeLength() - 1, (byte) value).array();
        };
    }
}
