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
import io.trino.filesystem.local.LocalInputFile;
import io.trino.metastore.HiveType;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.plugin.base.metrics.FileFormatDataSourceStats;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.plugin.hudi.file.HudiBaseFile;
import io.trino.spi.SplitWeight;
import io.trino.spi.connector.ColumnHandle;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.DynamicFilter;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.testing.MaterializedResult;
import io.trino.testing.TestingConnectorSession;
import org.apache.parquet.conf.PlainParquetConfiguration;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.io.LocalOutputFile;
import org.apache.parquet.schema.LogicalTypeAnnotation;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;
import org.joda.time.DateTimeZone;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.CompletableFuture;

import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hudi.HudiPageSourceProvider.createPageSource;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static org.apache.hudi.common.model.HoodieRecord.HOODIE_META_COLUMNS;
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
 * The fixture writes every data column as the type it had BEFORE the evolution and every handle carries the type the
 * metastore reports AFTER it, which is exactly the state an unrewritten base file is in. Values grow with the row
 * index so pruning stays observable: a predicate that survives pushdown reads fewer rows than the file holds, and one
 * that was dropped reads all of them. Do not "simplify" that to asserting the matching rows alone -- both a working
 * pushdown and no pushdown at all produce the same matching rows, since the connector's pushdown is an optimization
 * and the engine re-applies the predicate above the scan.
 */
class TestHudiEvolvedColumnPredicates
{
    private static final String STABLE_COLUMN = "stable_int";
    /** Written as parquet FLOAT, reported by the metastore as double. */
    private static final String FLOAT_TO_DOUBLE_COLUMN = "evolved_double";
    /** Written as parquet INT32, reported by the metastore as bigint. */
    private static final String INT_TO_BIGINT_COLUMN = "evolved_bigint";
    /** Written as parquet INT32, reported by the metastore as string. */
    private static final String INT_TO_VARCHAR_COLUMN = "evolved_varchar";

    private static final int ROW_COUNT = 1000;
    private static final long THRESHOLD = 900;
    private static final int MATCHING_ROW_COUNT = (int) (ROW_COUNT - THRESHOLD - 1);

    @TempDir
    static Path tempDir;

    private static Path baseFile;

    @BeforeAll
    static void writeBaseFile()
            throws IOException
    {
        MessageType schema = preEvolutionFileSchema();
        baseFile = tempDir.resolve("evolved_base_file.parquet");
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(baseFile))
                .withType(schema)
                .withConf(new PlainParquetConfiguration())
                .withRowGroupSize(1024L)
                .withPageSize(512)
                .build()) {
            for (int row = 0; row < ROW_COUNT; row++) {
                Group group = groupFactory.newGroup();
                for (String metaColumn : HOODIE_META_COLUMNS) {
                    group.append(metaColumn, metaColumn + "_" + row);
                }
                group.append(STABLE_COLUMN, row);
                group.append(FLOAT_TO_DOUBLE_COLUMN, (float) row);
                group.append(INT_TO_BIGINT_COLUMN, row);
                group.append(INT_TO_VARCHAR_COLUMN, row);
                writer.write(group);
            }
        }
        // With a single row group there would be nothing to prune and every "still prunes" assertion below would
        // hold without proving anything, so assert the outcome rather than the writer knobs that produce it.
        assertThat(rowGroupCount(baseFile)).as("row groups written").isGreaterThan(1);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnFloatColumnEvolvedToDouble(boolean useParquetColumnNames)
            throws Exception
    {
        HiveColumnHandle evolved = column(FLOAT_TO_DOUBLE_COLUMN, HiveType.HIVE_DOUBLE, DOUBLE);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        MaterializedResult result = read(projection, greaterThanThreshold(evolved, DOUBLE, (double) THRESHOLD),
                useParquetColumnNames, DynamicFilter.EMPTY);

        // The domain cannot be matched against FLOAT statistics, so it is dropped and nothing is pruned. Before the
        // fix this threw HUDI_BAD_DATA ("Corrupted statistics for column") instead of reading anything at all.
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        // The read itself promotes, so the rows the engine will filter carry the widened values
        assertThat(result.getMaterializedRows().get(7).getField(1)).as("promoted value of row 7").isEqualTo(7.0d);
        assertThat(valuesOver(result, 1)).as("rows matching %s > %s", FLOAT_TO_DOUBLE_COLUMN, THRESHOLD).isEqualTo(MATCHING_ROW_COUNT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnIntColumnEvolvedToVarchar(boolean useParquetColumnNames)
            throws Exception
    {
        HiveColumnHandle evolved = column(INT_TO_VARCHAR_COLUMN, HiveType.HIVE_STRING, VARCHAR);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        MaterializedResult result = read(projection,
                greaterThanThreshold(evolved, VARCHAR, Slices.utf8Slice("900")),
                useParquetColumnNames, DynamicFilter.EMPTY);

        // A varchar domain over an INT32 column would cast an Integer to a Slice
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        assertThat(result.getMaterializedRows().get(7).getField(1)).as("promoted value of row 7").isEqualTo("7");
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnIntColumnEvolvedToBigintStillPrunes(boolean useParquetColumnNames)
            throws Exception
    {
        HiveColumnHandle evolved = column(INT_TO_BIGINT_COLUMN, HiveType.HIVE_LONG, BIGINT);
        List<HiveColumnHandle> projection = List.of(column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER), evolved);

        MaterializedResult result = read(projection, greaterThanThreshold(evolved, BIGINT, THRESHOLD),
                useParquetColumnNames, DynamicFilter.EMPTY);

        // asLong takes an Integer as happily as a Long, so this promotion is one the statistics CAN answer and the
        // guard must leave it alone. This is what catches a check that drops more than it should.
        assertThat(result.getRowCount()).as("rows read out of %s", ROW_COUNT).isLessThan(ROW_COUNT);
        assertThat(valuesOver(result, 1)).as("rows matching %s > %s after pruning", INT_TO_BIGINT_COLUMN, THRESHOLD).isEqualTo(MATCHING_ROW_COUNT);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    public void testPredicateOnUnevolvedColumnStillPrunes(boolean useParquetColumnNames)
            throws Exception
    {
        HiveColumnHandle stable = column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER);
        List<HiveColumnHandle> projection = List.of(stable);

        MaterializedResult result = read(projection, greaterThanThreshold(stable, INTEGER, THRESHOLD),
                useParquetColumnNames, DynamicFilter.EMPTY);

        assertThat(result.getRowCount()).as("rows read out of %s", ROW_COUNT).isLessThan(ROW_COUNT);
        assertThat(valuesOver(result, 0)).as("rows matching %s > %s after pruning", STABLE_COLUMN, THRESHOLD).isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testOnlyTheEvolvedColumnsDomainIsDropped()
            throws Exception
    {
        HiveColumnHandle stable = column(STABLE_COLUMN, HiveType.HIVE_INT, INTEGER);
        HiveColumnHandle evolved = column(FLOAT_TO_DOUBLE_COLUMN, HiveType.HIVE_DOUBLE, DOUBLE);
        List<HiveColumnHandle> projection = List.of(stable, evolved);

        MaterializedResult result = read(projection,
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
        MaterializedResult result = read(projection, TupleDomain.all(), false,
                dynamicFilterOn(greaterThanThreshold(evolved, DOUBLE, (double) THRESHOLD)));

        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
    }

    /**
     * Reads the whole base file through the page source the connector builds for a split with no log files, which is
     * the only path on which it enables predicate pushdown.
     */
    private static MaterializedResult read(
            List<HiveColumnHandle> projection,
            TupleDomain<HiveColumnHandle> predicate,
            boolean useParquetColumnNames,
            DynamicFilter dynamicFilter)
            throws Exception
    {
        long fileSize = Files.size(baseFile);
        HudiSplit split = new HudiSplit(
                new HudiBaseFile(baseFile.toString(), baseFile.getFileName().toString(), fileSize, 0, 0, fileSize),
                List.of(),
                "000",
                predicate,
                List.of(),
                SplitWeight.standard());
        HudiSessionProperties sessionProperties = new HudiSessionProperties(
                new HudiConfig().setUseParquetColumnNames(useParquetColumnNames),
                new ParquetReaderConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();

        List<Type> types = projection.stream().map(HiveColumnHandle::getType).toList();
        try (ConnectorPageSource pageSource = createPageSource(
                session,
                projection,
                split,
                new LocalInputFile(baseFile.toFile()),
                baseFile.toString(),
                0L,
                fileSize,
                OptionalLong.of(fileSize),
                new FileFormatDataSourceStats(),
                ParquetReaderOptions.builder().build(),
                DateTimeZone.UTC,
                dynamicFilter,
                true)) {
            return materializeSourceDataStream(session, pageSource, types).toTestTypes();
        }
    }

    /**
     * The base file as it was written BEFORE the evolution: the five {@code _hoodie_*} meta columns followed by the
     * data columns in their original types. The metastore column list the handles below model reports the widened
     * types instead, which is the whole point of the fixture.
     */
    private static MessageType preEvolutionFileSchema()
    {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        for (String metaColumn : HOODIE_META_COLUMNS) {
            fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(metaColumn));
        }
        fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named(STABLE_COLUMN));
        fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.FLOAT, OPTIONAL).named(FLOAT_TO_DOUBLE_COLUMN));
        fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named(INT_TO_BIGINT_COLUMN));
        fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named(INT_TO_VARCHAR_COLUMN));
        return new MessageType("hudi_base_file", fields);
    }

    /**
     * A handle as the metastore reports the column AFTER the evolution, on its physical ordinal. Stale ordinals are
     * {@code TestHudiPageSourceProviderTest}'s subject, not this one, so the two resolution modes see the same
     * column here and any difference between them is about the type alone.
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

    private static int rowGroupCount(Path path)
            throws IOException
    {
        try (ParquetFileReader reader = ParquetFileReader.open(new org.apache.parquet.io.LocalInputFile(path))) {
            return reader.getRowGroups().size();
        }
    }

    private static DynamicFilter dynamicFilterOn(TupleDomain<HiveColumnHandle> predicate)
    {
        return new DynamicFilter()
        {
            @Override
            public Set<ColumnHandle> getColumnsCovered()
            {
                return Set.copyOf(predicate.getDomains().orElseThrow().keySet());
            }

            @Override
            public CompletableFuture<?> isBlocked()
            {
                return CompletableFuture.completedFuture(null);
            }

            @Override
            public boolean isComplete()
            {
                return true;
            }

            @Override
            public boolean isAwaitable()
            {
                return false;
            }

            @Override
            public TupleDomain<ColumnHandle> getCurrentPredicate()
            {
                return predicate.transformKeys(ColumnHandle.class::cast);
            }
        };
    }
}
