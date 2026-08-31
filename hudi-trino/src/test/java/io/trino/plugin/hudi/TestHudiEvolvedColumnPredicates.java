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
package io.trino.plugin.hudi;

import io.airlift.slice.Slices;
import io.trino.metastore.HiveType;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.dynamicFilterOn;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.read;
import static io.trino.plugin.hudi.TestingBaseFilePageSource.writeBaseFile;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Covers what a pushed-down predicate does to a base file written before the column it constrains was evolved.
 * <p>
 * Hudi lets a column's type widen and hive sync then reports the NEW type, while every base file written before the
 * evolution keeps storing the old one. The parquet reader copes with that on its own -- {@code ColumnReaderFactory}
 * decodes {@code FLOAT} into {@code DOUBLE} and {@code INT32} into {@code BIGINT}, and
 * {@code ParquetTypeTranslator.createCoercer} handles the rest -- but the statistics do not: {@code
 * TupleDomainParquetPredicate.getDomain} reads them as whatever the DOMAIN's type says, so a {@code DOUBLE} domain
 * over a {@code FLOAT} column casts a {@code Float} to a {@code Double} and fails the whole split with {@code
 * HUDI_BAD_DATA}. See apache/hudi#19457.
 * <p>
 * Two files, because that is the state one insert after an evolution leaves a table in: {@link #preEvolutionFile}
 * stores every data column as the type it had BEFORE, {@link #postEvolutionFile} as the type it has after, and the
 * handles carry the metastore's post-evolution types for both. A split over the first must drop its domain; a split
 * over the second must keep it and go on pruning, which is what stops the guard from being written as "give up on
 * this column".
 * <p>
 * Values grow with the row index so pruning stays observable: a predicate that survives pushdown reads fewer rows
 * than the file holds, and one that was dropped reads all of them. Do not "simplify" that to asserting the matching
 * rows alone -- both a working pushdown and no pushdown at all produce the same matching rows, since the connector's
 * pushdown is an optimization and the engine re-applies the predicate above the scan.
 */
class TestHudiEvolvedColumnPredicates
{
    private static final String STABLE_COLUMN = "stable_int";
    /** Written as parquet FLOAT before the evolution, DOUBLE after, reported by the metastore as double. */
    private static final String FLOAT_TO_DOUBLE_COLUMN = "evolved_double";
    /** Written as parquet INT32 before the evolution, INT64 after, reported by the metastore as bigint. */
    private static final String INT_TO_BIGINT_COLUMN = "evolved_bigint";
    /** Written as parquet INT32 before the evolution, a BINARY string after, reported by the metastore as string. */
    private static final String INT_TO_VARCHAR_COLUMN = "evolved_varchar";

    private static final int ROW_COUNT = 1000;
    private static final long THRESHOLD = 900;
    private static final int MATCHING_ROW_COUNT = (int) (ROW_COUNT - THRESHOLD - 1);
    /** A value that exists in both files, for the equality predicates that reach the bloom filter. */
    private static final long PRESENT_VALUE = 500;

    @TempDir
    static Path tempDir;

    private static Path preEvolutionFile;
    private static Path postEvolutionFile;

    @BeforeAll
    static void writeBaseFiles()
            throws IOException
    {
        preEvolutionFile = tempDir.resolve("pre_evolution_base_file.parquet");
        postEvolutionFile = tempDir.resolve("post_evolution_base_file.parquet");

        // Bloom filters on the two integral columns, as a Hudi writer builds for every column set in
        // parquet.bloom.filter.enabled#<column>. They are what makes an equality predicate on an evolved column
        // dangerous rather than merely unhelpful: see testEqualityOnAnIntColumnEvolvedToBigintStillFindsItsRows.
        List<String> bloomFilterColumns = List.of(STABLE_COLUMN, INT_TO_BIGINT_COLUMN);
        int preEvolutionRowGroups = writeBaseFile(preEvolutionFile, preEvolutionFileSchema(), ROW_COUNT, bloomFilterColumns,
                (group, row) -> {
                    appendMetaColumns(group, row);
                    group.append(STABLE_COLUMN, row);
                    group.append(FLOAT_TO_DOUBLE_COLUMN, (float) row);
                    group.append(INT_TO_BIGINT_COLUMN, row);
                    group.append(INT_TO_VARCHAR_COLUMN, row);
                });
        int postEvolutionRowGroups = writeBaseFile(postEvolutionFile, postEvolutionFileSchema(), ROW_COUNT, bloomFilterColumns,
                (group, row) -> {
                    appendMetaColumns(group, row);
                    group.append(STABLE_COLUMN, row);
                    group.append(FLOAT_TO_DOUBLE_COLUMN, (double) row);
                    group.append(INT_TO_BIGINT_COLUMN, (long) row);
                    group.append(INT_TO_VARCHAR_COLUMN, Integer.toString(row));
                });

        // With a single row group there would be nothing to prune and every "still prunes" assertion below would
        // hold without proving anything, so assert the outcome rather than the writer knobs that produce it.
        assertThat(preEvolutionRowGroups).as("row groups in the pre-evolution file").isGreaterThan(1);
        assertThat(postEvolutionRowGroups).as("row groups in the post-evolution file").isGreaterThan(1);
    }

    /**
     * The both-modes anchor. {@code column()} builds every handle on its physical ordinal, so the two values of
     * {@code hudi.parquet.use-column-names} hand the guard the identical descriptor and the axis cannot discriminate
     * anywhere in this class -- it is carried here alone, to pin that the guard runs after the resolution fork
     * rather than inside one branch of it. Stale ordinals are {@code TestHudiPageSourceProviderTest}'s subject.
     */
    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnFloatColumnEvolvedToDouble(boolean useParquetColumnNames)
            throws Exception
    {
        HiveColumnHandle evolved = column(FLOAT_TO_DOUBLE_COLUMN, HiveType.HIVE_DOUBLE, DOUBLE);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        MaterializedResult result = read(preEvolutionFile, projection, greaterThanThreshold(evolved, DOUBLE, (double) THRESHOLD),
                useParquetColumnNames, DynamicFilter.EMPTY);

        // The domain cannot be matched against FLOAT statistics, so it is dropped and nothing is pruned. Before the
        // fix this threw HUDI_BAD_DATA ("Corrupted statistics for column") instead of reading anything at all.
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        // The read itself promotes, so the rows the engine will filter carry the widened values
        assertThat(result.getMaterializedRows().get(7).getField(1)).as("promoted value of row 7").isEqualTo(7.0d);
        assertThat(valuesOver(result, 1)).as("rows matching %s > %s", FLOAT_TO_DOUBLE_COLUMN, THRESHOLD).isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testPredicateOnIntColumnEvolvedToVarchar()
            throws Exception
    {
        HiveColumnHandle evolved = column(INT_TO_VARCHAR_COLUMN, HiveType.HIVE_STRING, VARCHAR);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        MaterializedResult result = read(preEvolutionFile, projection,
                greaterThanThreshold(evolved, VARCHAR, Slices.utf8Slice("900")), false, DynamicFilter.EMPTY);

        // A varchar domain over an INT32 column would cast an Integer to a Slice
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        assertThat(result.getMaterializedRows().get(7).getField(1)).as("promoted value of row 7").isEqualTo("7");
    }

    /**
     * apache/hudi#19457 comment thread: {@code asLong} takes an Integer as happily as a Long, so the min/max side
     * reads this promotion perfectly -- and the guard still has to drop it, because {@code checkInBloomFilter} does
     * not. See {@link #testEqualityOnAnIntColumnEvolvedToBigintStillFindsItsRows} for what that costs when the pair
     * is kept. Losing row-group pruning on pre-evolution files is the price;
     * {@link #testThePostEvolutionFileStillPrunesTheSamePredicate} pins that files written after the evolution keep
     * it.
     */
    @Test
    public void testPredicateOnIntColumnEvolvedToBigintIsDropped()
            throws Exception
    {
        HiveColumnHandle evolved = column(INT_TO_BIGINT_COLUMN, HiveType.HIVE_LONG, BIGINT);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        MaterializedResult result = read(preEvolutionFile, projection, greaterThanThreshold(evolved, BIGINT, THRESHOLD),
                false, DynamicFilter.EMPTY);

        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        assertThat(valuesOver(result, 1)).as("rows matching %s > %s", INT_TO_BIGINT_COLUMN, THRESHOLD).isEqualTo(MATCHING_ROW_COUNT);
    }

    /**
     * The reason a bigint domain over an INT32 column has to be dropped even though its statistics read correctly.
     * <p>
     * {@code checkInBloomFilter} hashes the looked-up value at the DOMAIN's width, eight bytes for a bigint, while
     * parquet-mr hashed the INT32 column at four. The lookup therefore finds nothing,
     * {@code TupleDomainParquetPredicate.matches(BloomFilterStore, int)} reports no match, and every row group is
     * dropped with the matching row still inside it -- no error, just missing rows. Trino reads bloom filters by
     * default, so any Hudi table written with {@code parquet.bloom.filter.enabled#<column>=true} is exposed.
     */
    @Test
    public void testEqualityOnAnIntColumnEvolvedToBigintStillFindsItsRows()
            throws Exception
    {
        HiveColumnHandle evolved = column(INT_TO_BIGINT_COLUMN, HiveType.HIVE_LONG, BIGINT);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        MaterializedResult result = read(preEvolutionFile, projection,
                TupleDomain.withColumnDomains(Map.of(evolved, Domain.singleValue(BIGINT, PRESENT_VALUE))),
                false, DynamicFilter.EMPTY);

        // Dropped, so nothing is pruned -- and the row is there. With the pair kept the bloom filter answers "not
        // present" for every row group and this reads 0 rows.
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        assertThat(valuesEqualTo(result, 1)).as("rows holding %s = %s", INT_TO_BIGINT_COLUMN, PRESENT_VALUE).isEqualTo(1);
    }

    /**
     * The same predicate against a file written AFTER the evolution, where the column really is an INT64 and both
     * the statistics and the bloom filter answer it. Without this the guard could be "never push down on a column
     * the metastore widened" and every test above would still pass, while every table would lose pruning forever.
     */
    @Test
    public void testThePostEvolutionFileStillPrunesTheSamePredicate()
            throws Exception
    {
        HiveColumnHandle evolved = column(INT_TO_BIGINT_COLUMN, HiveType.HIVE_LONG, BIGINT);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);
        TupleDomain<HiveColumnHandle> predicate = TupleDomain.withColumnDomains(
                Map.of(evolved, Domain.singleValue(BIGINT, PRESENT_VALUE)));

        MaterializedResult result = read(postEvolutionFile, projection, predicate, false, DynamicFilter.EMPTY);

        assertThat(result.getRowCount()).as("rows read out of %s", ROW_COUNT).isLessThan(ROW_COUNT);
        assertThat(valuesEqualTo(result, 1)).as("rows holding %s = %s", INT_TO_BIGINT_COLUMN, PRESENT_VALUE).isEqualTo(1);

        // ... and the range predicate the pre-evolution file could not prune on at all
        MaterializedResult rangeResult = read(postEvolutionFile, projection,
                greaterThanThreshold(evolved, BIGINT, THRESHOLD), false, DynamicFilter.EMPTY);
        assertThat(rangeResult.getRowCount()).as("rows read out of %s", ROW_COUNT).isLessThan(ROW_COUNT);
        assertThat(valuesOver(rangeResult, 1)).as("rows matching %s > %s after pruning", INT_TO_BIGINT_COLUMN, THRESHOLD)
                .isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testOnlyTheEvolvedColumnsDomainIsDropped()
            throws Exception
    {
        HiveColumnHandle stable = column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle evolved = column(FLOAT_TO_DOUBLE_COLUMN, HiveType.HIVE_DOUBLE, DOUBLE);
        List<HiveColumnHandle> projection = List.of(stable, evolved);

        MaterializedResult result = read(preEvolutionFile, projection,
                greaterThanThreshold(stable, INTEGER, THRESHOLD)
                        .intersect(greaterThanThreshold(evolved, DOUBLE, (double) THRESHOLD)),
                false, DynamicFilter.EMPTY);

        // One unusable domain must not cost the whole predicate its pushdown: the stable column's domain still
        // prunes, which is only visible because reading everything and reading nothing are both wrong here.
        assertThat(result.getRowCount()).as("rows read out of %s", ROW_COUNT).isLessThan(ROW_COUNT);
        assertThat(valuesOver(result, 0)).as("rows matching %s > %s after pruning", STABLE_COLUMN, THRESHOLD).isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testEvolvedColumnArrivingThroughADynamicFilter()
            throws Exception
    {
        HiveColumnHandle evolved = column(FLOAT_TO_DOUBLE_COLUMN, HiveType.HIVE_DOUBLE, DOUBLE);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        // A dynamic filter reaches getCombinedPredicate by its own route and its handles carry the metastore type
        // just the same, so it has to be guarded on the same path
        MaterializedResult result = read(preEvolutionFile, projection, TupleDomain.all(), false,
                dynamicFilterOn(greaterThanThreshold(evolved, DOUBLE, (double) THRESHOLD)));

        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
    }

    /**
     * The control for the test above, which on its own cannot tell a guarded dynamic filter from one that never
     * reached {@code getPushdownPredicate} at all -- both read every row. Putting the same shape of filter on a
     * column the guard keeps has to prune, so a dynamic filter that quietly stopped being pushed down fails here.
     */
    @Test
    public void testDynamicFilterOnTheStableColumnStillPrunes()
            throws Exception
    {
        HiveColumnHandle stable = column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER);
        List<HiveColumnHandle> projection = List.of(stable);

        MaterializedResult result = read(preEvolutionFile, projection, TupleDomain.all(), false,
                dynamicFilterOn(greaterThanThreshold(stable, INTEGER, THRESHOLD)));

        assertThat(result.getRowCount()).as("rows read out of %s", ROW_COUNT).isLessThan(ROW_COUNT);
        assertThat(valuesOver(result, 0)).as("rows matching %s > %s after pruning", STABLE_COLUMN, THRESHOLD).isEqualTo(MATCHING_ROW_COUNT);
    }

    private static void appendMetaColumns(org.apache.parquet.example.data.Group group, int row)
    {
        for (String metaColumn : HOODIE_META_COLUMNS) {
            group.append(metaColumn, metaColumn + "_" + row);
        }
    }

    /**
     * The base file as it was written BEFORE the evolution: the five {@code _hoodie_*} meta columns followed by the
     * data columns in their original types. The metastore column list the handles below model reports the widened
     * types instead, which is the whole point of the fixture.
     */
    private static MessageType preEvolutionFileSchema()
    {
        return fileSchema(
                Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, OPTIONAL).named(FLOAT_TO_DOUBLE_COLUMN),
                Types.primitive(INT32, OPTIONAL).named(INT_TO_BIGINT_COLUMN),
                Types.primitive(INT32, OPTIONAL).named(INT_TO_VARCHAR_COLUMN));
    }

    /** The same table one insert later: every column now written as the type the metastore already reported. */
    private static MessageType postEvolutionFileSchema()
    {
        return fileSchema(
                Types.primitive(PrimitiveType.PrimitiveTypeName.DOUBLE, OPTIONAL).named(FLOAT_TO_DOUBLE_COLUMN),
                Types.primitive(INT64, OPTIONAL).named(INT_TO_BIGINT_COLUMN),
                Types.primitive(BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(INT_TO_VARCHAR_COLUMN));
    }

    private static MessageType fileSchema(org.apache.parquet.schema.Type... evolvedColumns)
    {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        for (String metaColumn : HOODIE_META_COLUMNS) {
            fields.add(Types.primitive(BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(metaColumn));
        }
        fields.add(Types.primitive(INT32, OPTIONAL).named(STABLE_COLUMN));
        fields.addAll(List.of(evolvedColumns));
        return new MessageType("hudi_base_file", fields);
    }

    /**
     * A handle as the metastore reports the column AFTER the evolution, on its physical ordinal. Both files lay the
     * columns out identically, so one handle serves either.
     */
    private static HiveColumnHandle column(String columnName, HiveType hiveType, Type trinoType)
    {
        return createBaseColumn(columnName, physicalIndexOf(columnName), hiveType, trinoType,
                HiveColumnHandle.ColumnType.REGULAR, Optional.empty());
    }

    private static int physicalIndexOf(String columnName)
    {
        List<org.apache.parquet.schema.Type> fields = preEvolutionFileSchema().getFields();
        for (int i = 0; i < fields.size(); i++) {
            if (fields.get(i).getName().equals(columnName)) {
                return i;
            }
        }
        throw new IllegalArgumentException("No such column in the fixture: " + columnName);
    }

    private static TupleDomain<HiveColumnHandle> greaterThanThreshold(HiveColumnHandle handle, Type type, Object threshold)
    {
        return TupleDomain.withColumnDomains(Map.of(handle,
                Domain.create(ValueSet.ofRanges(Range.greaterThan(type, threshold)), false)));
    }

    /** Counts the rows whose {@code fieldIndex}-th field is over {@link #THRESHOLD}, whatever numeric type it read as. */
    private static long valuesOver(MaterializedResult result, int fieldIndex)
    {
        return result.getMaterializedRows().stream()
                .map(row -> row.getField(fieldIndex))
                .filter(value -> value != null && ((Number) value).doubleValue() > THRESHOLD)
                .count();
    }

    private static long valuesEqualTo(MaterializedResult result, int fieldIndex)
    {
        return result.getMaterializedRows().stream()
                .map(row -> row.getField(fieldIndex))
                .filter(value -> value != null && ((Number) value).longValue() == PRESENT_VALUE)
                .count();
    }
}
