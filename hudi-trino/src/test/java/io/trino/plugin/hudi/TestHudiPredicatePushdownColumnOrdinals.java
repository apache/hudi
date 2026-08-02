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

import io.trino.filesystem.local.LocalInputFile;
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

import static io.trino.metastore.HiveType.HIVE_INT;
import static io.trino.plugin.hive.HiveColumnHandle.ColumnType.REGULAR;
import static io.trino.plugin.hive.HiveColumnHandle.createBaseColumn;
import static io.trino.plugin.hudi.HudiPageSourceProvider.createPageSource;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.testing.MaterializedResult.materializeSourceDataStream;
import static java.lang.Integer.parseInt;
import static org.apache.parquet.schema.Type.Repetition.OPTIONAL;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Reads a base file whose physical column order does not match the metastore's, the layout hive sync produces
 * with {@code hoodie.datasource.hive_sync.omit_metadata_fields=true}: the five {@code _hoodie_*} meta columns are
 * absent from the metastore, so every data column's metastore ordinal is five below its physical position.
 * <p>
 * With {@code hudi.parquet.use-column-names=false} the parquet page source resolves columns positionally, so a
 * predicate whose handle still carries the metastore ordinal lands on whichever column physically sits there and
 * row groups get pruned on that column's statistics. The fixture makes that observable: {@code c7} grows with the
 * row index while every other data column stays in 0..9, so a domain meant for {@code c7} but applied to any other
 * column excludes every row group and the read returns nothing.
 * <p>
 * Note that the shadowed column has to be part of the PROJECTION for the damage to appear: {@code
 * descriptorsByPath} is derived from the projection, so a domain resolving to a column the query does not read
 * finds no descriptor and is discarded instead. Do not "simplify" the projections below to the predicate column
 * alone - that turns these tests green against the unfixed code.
 */
class TestHudiPredicatePushdownColumnOrdinals
{
    private static final List<String> META_COLUMNS = List.of(
            "_hoodie_commit_time",
            "_hoodie_commit_seqno",
            "_hoodie_record_key",
            "_hoodie_partition_path",
            "_hoodie_file_name");
    private static final int DATA_COLUMN_COUNT = 10;
    /** The column the predicate is on: physically at 12, but numbered 7 by a metastore without the meta columns. */
    private static final String PREDICATE_COLUMN = "c7";
    /** The column physically sitting at {@code c7}'s stale ordinal, and therefore the one that shadows it. */
    private static final String SHADOWED_COLUMN = "c2";
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
        MessageType schema = fileSchema();
        baseFile = tempDir.resolve("base_file.parquet");
        SimpleGroupFactory groupFactory = new SimpleGroupFactory(schema);
        try (ParquetWriter<Group> writer = ExampleParquetWriter.builder(new LocalOutputFile(baseFile))
                .withType(schema)
                .withConf(new PlainParquetConfiguration())
                .withRowGroupSize(1024L)
                .withPageSize(512)
                .build()) {
            for (int row = 0; row < ROW_COUNT; row++) {
                Group group = groupFactory.newGroup();
                for (String metaColumn : META_COLUMNS) {
                    group.append(metaColumn, metaColumn + "_" + row);
                }
                for (int column = 0; column < DATA_COLUMN_COUNT; column++) {
                    String columnName = "c" + column;
                    group.append(columnName, columnName.equals(PREDICATE_COLUMN) ? row : row % 10);
                }
                writer.write(group);
            }
        }
        // The writer flushes a row group whenever the buffered size is over withRowGroupSize, checked every
        // parquet.page.size.row.check.min records (100 by default), which is what actually splits this file.
        // Assert the outcome rather than the knobs: with a single row group there would be nothing to prune,
        // and every test below would pass without proving anything.
        assertThat(rowGroupCount(baseFile)).as("row groups written").isGreaterThan(1);
    }

    @Test
    public void testPredicateOnStaleOrdinalKeepsMatchingRows()
            throws Exception
    {
        List<HiveColumnHandle> projection = List.of(dataColumn(SHADOWED_COLUMN), dataColumn(PREDICATE_COLUMN));

        MaterializedResult result = read(projection, greaterThanThreshold(PREDICATE_COLUMN), false, DynamicFilter.EMPTY);

        // The shadowed column never leaves 0..9, so a domain of "> 900" applied to it prunes every row group
        assertThat(matchingRowCount(result, projection, PREDICATE_COLUMN))
                .as("rows matching %s > %s", PREDICATE_COLUMN, THRESHOLD)
                .isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testPredicateOnStaleOrdinalStillPrunesRowGroups()
            throws Exception
    {
        List<HiveColumnHandle> projection = List.of(dataColumn(SHADOWED_COLUMN), dataColumn(PREDICATE_COLUMN));

        MaterializedResult result = read(projection, greaterThanThreshold(PREDICATE_COLUMN), false, DynamicFilter.EMPTY);

        // Correct results alone would also be produced by pushing nothing down; reading fewer rows than the file
        // holds is only possible if the domain reached the column it was written for, and the matching rows must
        // survive that pruning
        assertThat(result.getRowCount())
                .as("rows read out of %s", ROW_COUNT)
                .isLessThan(ROW_COUNT);
        assertThat(matchingRowCount(result, projection, PREDICATE_COLUMN))
                .as("rows matching %s > %s after pruning", PREDICATE_COLUMN, THRESHOLD)
                .isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testStaleOrdinalArrivingThroughADynamicFilter()
            throws Exception
    {
        List<HiveColumnHandle> projection = List.of(dataColumn(SHADOWED_COLUMN), dataColumn(PREDICATE_COLUMN));

        // A dynamic filter reaches getCombinedPredicate by its own route, and its handles carry the same stale
        // metastore ordinals the split's predicate does
        MaterializedResult result = read(projection, TupleDomain.all(), false,
                dynamicFilterOn(greaterThanThreshold(PREDICATE_COLUMN)));

        assertThat(matchingRowCount(result, projection, PREDICATE_COLUMN))
                .as("rows matching a dynamic filter of %s > %s", PREDICATE_COLUMN, THRESHOLD)
                .isEqualTo(MATCHING_ROW_COUNT);
    }

    @Test
    public void testPredicateOnColumnAddedAfterBaseFileWasWritten()
            throws Exception
    {
        // The metastore carries one column more than this base file does, numbered 10 - an ordinal that is still
        // in range physically, where it picks out "c5"
        String addedColumn = "c" + DATA_COLUMN_COUNT;
        List<HiveColumnHandle> projection = List.of(dataColumn("c5"), dataColumn(PREDICATE_COLUMN), dataColumn(addedColumn));

        // IS NULL, not a range: the added column is null in every row of this base file, so this predicate is
        // satisfied by all of them. A range predicate would be unsatisfiable here and the buggy read's empty
        // result would be the right answer by accident.
        MaterializedResult result = read(projection,
                TupleDomain.withColumnDomains(Map.of(dataColumn(addedColumn), Domain.onlyNull(INTEGER))),
                false, DynamicFilter.EMPTY);

        // A column the file does not carry has to be dropped from the pushed-down predicate. Pushed positionally
        // it would land on "c5", which has no nulls at all, and every row group would be pruned.
        assertThat(result.getRowCount()).as("rows read").isEqualTo(ROW_COUNT);
        assertThat(result.getMaterializedRows().getFirst().getField(2)).as("value of %s", addedColumn).isNull();
    }

    @Test
    public void testPositionalAndNameBasedResolutionAgree()
            throws Exception
    {
        List<HiveColumnHandle> projection = List.of(dataColumn(SHADOWED_COLUMN), dataColumn(PREDICATE_COLUMN));
        TupleDomain<HiveColumnHandle> predicate = greaterThanThreshold(PREDICATE_COLUMN);

        MaterializedResult positional = read(projection, predicate, false, DynamicFilter.EMPTY);
        MaterializedResult byName = read(projection, predicate, true, DynamicFilter.EMPTY);

        // Anchor the comparison: both modes regressing to no pushdown at all would otherwise agree happily
        assertThat(byName.getRowCount()).as("rows read with use-column-names=true").isLessThan(ROW_COUNT);
        assertThat(positional.getMaterializedRows())
                .as("hudi.parquet.use-column-names=false must read what use-column-names=true reads")
                .isEqualTo(byName.getMaterializedRows());
    }

    /**
     * Reads the whole base file through the page source the connector builds for a split with no log files, which
     * is the only path on which it enables predicate pushdown.
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

    private static MessageType fileSchema()
    {
        List<org.apache.parquet.schema.Type> fields = new ArrayList<>();
        for (String metaColumn : META_COLUMNS) {
            fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.BINARY, OPTIONAL).as(LogicalTypeAnnotation.stringType()).named(metaColumn));
        }
        for (int column = 0; column < DATA_COLUMN_COUNT; column++) {
            fields.add(Types.primitive(PrimitiveType.PrimitiveTypeName.INT32, OPTIONAL).named("c" + column));
        }
        return new MessageType("hudi_base_file", fields);
    }

    /**
     * Builds the handle a metastore without the Hudi meta columns produces: numbered by its position among the
     * data columns alone, which is {@link #META_COLUMNS} short of its physical position.
     */
    private static HiveColumnHandle dataColumn(String columnName)
    {
        return createBaseColumn(columnName, parseInt(columnName.substring(1)), HIVE_INT, INTEGER, REGULAR, Optional.empty());
    }

    private static TupleDomain<HiveColumnHandle> greaterThanThreshold(String columnName)
    {
        return TupleDomain.withColumnDomains(Map.of(
                dataColumn(columnName),
                Domain.create(ValueSet.ofRanges(Range.greaterThan(INTEGER, THRESHOLD)), false)));
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

    private static long matchingRowCount(MaterializedResult result, List<HiveColumnHandle> projection, String columnName)
    {
        int fieldIndex = projection.indexOf(dataColumn(columnName));
        return result.getMaterializedRows().stream()
                .map(row -> row.getField(fieldIndex))
                .filter(value -> value != null && ((Number) value).longValue() > THRESHOLD)
                .count();
    }

    private static int rowGroupCount(Path path)
            throws IOException
    {
        try (ParquetFileReader reader = ParquetFileReader.open(new org.apache.parquet.io.LocalInputFile(path))) {
            return reader.getRowGroups().size();
        }
    }
}
