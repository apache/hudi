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

import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Type;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.statistics.Statistics;
import org.apache.parquet.schema.LogicalTypeAnnotation;
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

import static io.trino.plugin.hudi.util.ParquetStatisticsDomains.dropIncomparableDomains;
import static io.trino.plugin.hudi.util.ParquetStatisticsDomains.hasComparableStatistics;
import static io.trino.plugin.hudi.util.TestParquetStatisticsDomains.LibraryOutcome.ALL;
import static io.trino.plugin.hudi.util.TestParquetStatisticsDomains.LibraryOutcome.NARROW;
import static io.trino.plugin.hudi.util.TestParquetStatisticsDomains.LibraryOutcome.THROWS;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TinyintType.TINYINT;
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
 * Pins {@link ParquetStatisticsDomains#hasComparableStatistics} against the method it exists to protect. Every case
 * below states BOTH what the guard decides and what {@code TupleDomainParquetPredicate.getDomain} actually does with
 * the same pair, and the check is run against the real {@code getDomain}, not a description of it. A Trino upgrade
 * that moves a branch therefore fails here, where the mismatch is a line of test output, instead of in a query.
 * <p>
 * The three library outcomes are worth telling apart, because the guard exists for two different reasons:
 * <ul>
 *     <li>{@link LibraryOutcome#THROWS} - the cast fails and the whole split dies with {@code HUDI_BAD_DATA}. This
 *     is apache/hudi#19457 as reported.</li>
 *     <li>{@link LibraryOutcome#NARROW} on a pair the guard drops - far worse: a domain IS produced, from bytes that
 *     mean something else entirely, and row groups get pruned on a comparison that is simply false. Nothing fails,
 *     rows just go missing.</li>
 *     <li>{@link LibraryOutcome#ALL} - the library declines the pair itself, so dropping it changes nothing.</li>
 * </ul>
 * The invariant that ties them together is asserted for every case: whatever the guard keeps must be a pair the
 * library reads a real range out of.
 */
class TestParquetStatisticsDomains
{
    private static final ParquetDataSourceId DATA_SOURCE_ID = new ParquetDataSourceId("test");
    private static final long VALUE_COUNT = 10;

    enum LibraryOutcome
    {
        /** getDomain read the statistics and returned a range narrower than "any value". */
        NARROW,
        /** getDomain declined to use the statistics and returned a domain covering every value. */
        ALL,
        /** getDomain failed, which the connector reports as a corrupt-statistics error over the whole split. */
        THROWS,
    }

    private record TypePair(String description, Type domainType, PrimitiveType fileType, boolean comparable, LibraryOutcome outcome)
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
                new TypePair("boolean over BOOLEAN", BOOLEAN, plain(PrimitiveTypeName.BOOLEAN), true, NARROW),
                new TypePair("integer over INT32", INTEGER, plain(INT32), true, NARROW),
                new TypePair("bigint over INT64", BIGINT, plain(INT64), true, NARROW),
                new TypePair("tinyint over INT32", TINYINT, plain(INT32), true, NARROW),
                new TypePair("date over INT32 date", DATE, annotated(INT32, LogicalTypeAnnotation.dateType()), true, NARROW),
                new TypePair("real over FLOAT", REAL, plain(FLOAT), true, NARROW),
                new TypePair("double over DOUBLE", DOUBLE, plain(PrimitiveTypeName.DOUBLE), true, NARROW),
                new TypePair("varchar over BINARY string", VARCHAR, annotated(BINARY, LogicalTypeAnnotation.stringType()), true, NARROW),
                new TypePair("decimal(9,2) over INT32 decimal(9,2)", DecimalType.createDecimalType(9, 2), decimal(INT32, 9, 2), true, NARROW),
                new TypePair("timestamp over INT64 timestamp", TIMESTAMP_MILLIS, annotated(INT64, LogicalTypeAnnotation.timestampType(false, TimeUnit.MILLIS)), true, NARROW),
                new TypePair("timestamp over INT96", TIMESTAMP_MILLIS, plain(INT96), true, NARROW),

                // Promotions the statistics can answer, so pushdown must survive them
                new TypePair("int -> long", BIGINT, plain(INT32), true, NARROW),
                new TypePair("decimal(9,2) -> decimal(9,4)", DecimalType.createDecimalType(9, 4), decimal(INT32, 9, 2), true, NARROW),
                new TypePair("decimal(20,2) -> decimal(38,4)", DecimalType.createDecimalType(38, 4), decimal(FIXED_LEN_BYTE_ARRAY, 20, 2), true, NARROW),
                new TypePair("integer over a zero-scale INT32 decimal", INTEGER, decimal(INT32, 9, 0), true, NARROW),

                // Promotions that fail the split today: apache/hudi#19457 and its neighbours
                new TypePair("float -> double", DOUBLE, plain(FLOAT), false, THROWS),
                new TypePair("int -> double", DOUBLE, plain(INT32), false, THROWS),
                new TypePair("long -> double", DOUBLE, plain(INT64), false, THROWS),
                new TypePair("int -> float", REAL, plain(INT32), false, THROWS),
                new TypePair("int -> string", VARCHAR, plain(INT32), false, THROWS),
                new TypePair("long -> string", VARCHAR, plain(INT64), false, THROWS),
                new TypePair("float -> string", VARCHAR, plain(FLOAT), false, THROWS),
                new TypePair("double -> string", VARCHAR, plain(PrimitiveTypeName.DOUBLE), false, THROWS),
                new TypePair("string -> date", DATE, annotated(BINARY, LogicalTypeAnnotation.stringType()), false, THROWS),

                // Promotions that silently prune on a comparison that means nothing, which is why the guard cannot
                // be a try/catch around the cast
                new TypePair("decimal -> string", VARCHAR, decimal(FIXED_LEN_BYTE_ARRAY, 20, 2), false, NARROW),
                new TypePair("string -> decimal", DecimalType.createDecimalType(9, 2), annotated(BINARY, LogicalTypeAnnotation.stringType()), false, NARROW),
                new TypePair("int -> decimal", DecimalType.createDecimalType(9, 2), plain(INT32), false, NARROW),
                new TypePair("integer over a scaled INT32 decimal", INTEGER, decimal(INT32, 9, 2), false, NARROW),

                // Pairs the library declines on its own, where dropping costs nothing
                new TypePair("timestamp over an unannotated INT64", TIMESTAMP_MILLIS, plain(INT64), false, ALL),
                new TypePair("varbinary over BINARY", VARBINARY, plain(BINARY), false, ALL));
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
    }

    @ParameterizedTest
    @MethodSource("typePairs")
    public void testEveryKeptPairYieldsUsableStatistics(TypePair pair)
    {
        // The invariant behind the whole table: a false negative only costs pruning, but a false positive is either
        // a failed query or a wrong one, so nothing may be kept that the library does not read a real range out of.
        if (hasComparableStatistics(pair.domainType(), pair.fileType())) {
            assertThat(pair.outcome()).as("library outcome for the kept pair %s", pair).isEqualTo(NARROW);
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

    private static PrimitiveType plain(PrimitiveTypeName primitiveTypeName)
    {
        if (primitiveTypeName == INT96) {
            return Types.primitive(primitiveTypeName, OPTIONAL).named("c");
        }
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

    /**
     * Statistics holding a small, non-degenerate range, so that a pair the library CAN read produces a domain
     * narrower than "any value" and the {@link LibraryOutcome#NARROW} cases stay distinguishable from
     * {@link LibraryOutcome#ALL}. Two nearby values also keep every integral type clear of
     * {@code isStatisticsOverflow}, which would otherwise widen tinyint back to everything.
     */
    private static Statistics<?> statisticsOf(PrimitiveType fileType)
    {
        return Statistics.getBuilderForReading(fileType)
                .withMin(statisticsBytes(fileType, 1))
                .withMax(statisticsBytes(fileType, 2))
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
            case BINARY -> Integer.toString(value).getBytes(UTF_8);
            // A decimal's unscaled value is big-endian two's complement, padded to the column's length
            case FIXED_LEN_BYTE_ARRAY -> ByteBuffer.allocate(fileType.getTypeLength()).put(fileType.getTypeLength() - 1, (byte) value).array();
        };
    }
}
