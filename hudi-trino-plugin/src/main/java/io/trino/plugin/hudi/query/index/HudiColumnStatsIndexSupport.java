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

import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.Duration;
import io.trino.parquet.predicate.TupleDomainParquetPredicate;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.TupleDomainUtils;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.Range;
import io.trino.spi.predicate.TupleDomain;
import io.trino.spi.predicate.ValueSet;
import io.trino.spi.type.Type;
import io.trino.spi.type.VarcharType;
import org.apache.avro.generic.GenericRecord;
import org.apache.hudi.avro.model.HoodieMetadataColumnStats;
import org.apache.hudi.common.model.BaseFile;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.hash.ColumnIndexID;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import static io.trino.parquet.predicate.PredicateUtils.isStatisticsOverflow;
import static io.trino.plugin.hudi.HudiSessionProperties.getColumnStatsWaitTimeout;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.spi.type.BooleanType.BOOLEAN;
import static io.trino.spi.type.DateType.DATE;
import static io.trino.spi.type.DoubleType.DOUBLE;
import static io.trino.spi.type.IntegerType.INTEGER;
import static io.trino.spi.type.RealType.REAL;
import static io.trino.spi.type.SmallintType.SMALLINT;
import static io.trino.spi.type.TinyintType.TINYINT;
import static java.lang.Float.floatToRawIntBits;

public class HudiColumnStatsIndexSupport
        extends HudiBaseIndexSupport
{
    private static final Logger log = Logger.get(HudiColumnStatsIndexSupport.class);
    private final CompletableFuture<Optional<Map<String, Map<String, HoodieMetadataColumnStats>>>> statsByFileNameFuture;
    protected final TupleDomain<String> regularColumnPredicates;
    private final List<String> regularColumns;
    private final Duration columnStatsWaitTimeout;
    private final long futureStartTimeMs;

    public HudiColumnStatsIndexSupport(ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<HiveColumnHandle> regularColumnPredicates)
    {
        this(log, session, schemaTableName, lazyMetaClient, lazyTableMetadata, regularColumnPredicates);
    }

    public HudiColumnStatsIndexSupport(Logger log, ConnectorSession session, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient, Lazy<HoodieTableMetadata> lazyTableMetadata, TupleDomain<HiveColumnHandle> regularColumnPredicates)
    {
        super(log, schemaTableName, lazyMetaClient);
        this.columnStatsWaitTimeout = getColumnStatsWaitTimeout(session);
        this.regularColumnPredicates = regularColumnPredicates.transformKeys(HiveColumnHandle::getName);
        this.regularColumns = this.regularColumnPredicates
                .getDomains().get().entrySet().stream().map(Map.Entry::getKey).collect(Collectors.toList());
        if (regularColumnPredicates.isAll() || !regularColumnPredicates.getDomains().isPresent()) {
            this.statsByFileNameFuture = CompletableFuture.completedFuture(Optional.empty());
        }
        else {
            // Get filter columns
            List<String> encodedTargetColumnNames = regularColumns
                    .stream()
                    .map(col -> new ColumnIndexID(col).asBase64EncodedString()).collect(Collectors.toList());
            statsByFileNameFuture = CompletableFuture.supplyAsync(() -> {
                HoodieTimer timer = HoodieTimer.start();
                if (!lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                        .contains(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS)) {
                    return Optional.empty();
                }

                Map<String, Map<String, HoodieMetadataColumnStats>> statsByFileName =
                        lazyTableMetadata.get().getRecordsByKeyPrefixes(encodedTargetColumnNames,
                                        HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS, true)
                                .collectAsList()
                                .stream()
                                .filter(f -> f.getData().getColumnStatMetadata().isPresent())
                                .map(f -> f.getData().getColumnStatMetadata().get())
                                .collect(Collectors.groupingBy(
                                        HoodieMetadataColumnStats::getFileName,
                                        Collectors.toMap(
                                                HoodieMetadataColumnStats::getColumnName,
                                                Function.identity())));
                log.debug("Column stats lookup took %s ms and identified %d relevant file IDs.", timer.endTimer(), statsByFileName.size());

                return Optional.of(statsByFileName);
            });
        }
        this.futureStartTimeMs = System.currentTimeMillis();
    }

    @Override
    public boolean shouldSkipFileSlice(FileSlice slice)
    {
        try {
            if (statsByFileNameFuture.isDone()) {
                Optional<Map<String, Map<String, HoodieMetadataColumnStats>>> statsOpt = statsByFileNameFuture.get();
                return statsOpt
                        .map(stats -> shouldSkipFileSlice(slice, stats, regularColumnPredicates, regularColumns))
                        .orElse(false);
            }

            long elapsedMs = System.currentTimeMillis() - futureStartTimeMs;
            if (elapsedMs > columnStatsWaitTimeout.toMillis()) {
                // Took too long; skip decision
                return false;
            }

            // If still within the timeout window, wait up to the remaining time
            long remainingMs = Math.max(0, columnStatsWaitTimeout.toMillis() - elapsedMs);
            Optional<Map<String, Map<String, HoodieMetadataColumnStats>>> statsOpt =
                    statsByFileNameFuture.get(remainingMs, TimeUnit.MILLISECONDS);

            return statsOpt
                    .map(stats -> shouldSkipFileSlice(slice, stats, regularColumnPredicates, regularColumns))
                    .orElse(false);
        }
        catch (TimeoutException | InterruptedException | ExecutionException e) {
            return false;
        }
    }

    @Override
    public boolean canApply(TupleDomain<String> tupleDomain)
    {
        boolean isIndexSupported = isIndexSupportAvailable();
        // indexDefinition is only available after table version EIGHT
        // For tables that have versions < EIGHT, column stats index is available as long as partition in metadata is available
        if (!isIndexSupported || lazyMetaClient.get().getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
            log.debug("Column Stats Index partition is not enabled in metadata.");
            return isIndexSupported;
        }

        Map<String, HoodieIndexDefinition> indexDefinitions = getAllIndexDefinitions();
        HoodieIndexDefinition colStatsDefinition = indexDefinitions.get(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
        if (colStatsDefinition == null || colStatsDefinition.getSourceFields() == null || colStatsDefinition.getSourceFields().isEmpty()) {
            log.warn("Column stats index definition is missing or has no source fields defined");
            return false;
        }

        // Optimization applied: Only consider applicable if predicates reference indexed columns
        List<String> sourceFields = colStatsDefinition.getSourceFields();
        boolean applicable = TupleDomainUtils.areSomeFieldsReferenced(tupleDomain, sourceFields);

        if (applicable) {
            log.debug("Column Stats Index is available and applicable (predicates reference indexed columns).");
        }
        else {
            log.debug("Column Stats Index is available, but predicates do not reference any indexed columns.");
        }
        return applicable;
    }

    public boolean isIndexSupportAvailable()
    {
        return lazyMetaClient.get().getTableConfig().getMetadataPartitions()
                .contains(HoodieTableMetadataUtil.PARTITION_NAME_COLUMN_STATS);
    }

    // TODO: Move helper functions below to TupleDomain/DomainUtils
    private static boolean shouldSkipFileSlice(
            FileSlice fileSlice,
            Map<String, Map<String, HoodieMetadataColumnStats>> statsByFileName,
            TupleDomain<String> regularColumnPredicates,
            List<String> regularColumns)
    {
        String fileSliceName = fileSlice.getBaseFile().map(BaseFile::getFileName).orElse("");
        // If no stats exist for this specific file, we cannot prune it.
        if (!statsByFileName.containsKey(fileSliceName)) {
            return false;
        }
        Map<String, HoodieMetadataColumnStats> stats = statsByFileName.get(fileSliceName);
        return !evaluateStatisticPredicate(regularColumnPredicates, stats, regularColumns);
    }

    protected static boolean evaluateStatisticPredicate(
            TupleDomain<String> regularColumnPredicates,
            Map<String, HoodieMetadataColumnStats> stats,
            List<String> regularColumns)
    {
        if (regularColumnPredicates.isNone() || !regularColumnPredicates.getDomains().isPresent()) {
            return true;
        }
        for (String regularColumn : regularColumns) {
            Domain columnPredicate = regularColumnPredicates.getDomains().get().get(regularColumn);
            Optional<HoodieMetadataColumnStats> currentColumnStats = Optional.ofNullable(stats.get(regularColumn));
            if (currentColumnStats.isEmpty()) {
                // No stats for column
            }
            else {
                Domain domain = getDomain(regularColumn, columnPredicate.getType(), currentColumnStats.get());
                if (columnPredicate.intersect(domain).isNone()) {
                    return false;
                }
            }
        }
        return true;
    }

    private static Domain getDomain(String colName, Type type, HoodieMetadataColumnStats statistics)
    {
        if (statistics == null) {
            return Domain.all(type);
        }
        boolean hasNullValue = statistics.getNullCount() != 0L;
        boolean hasNonNullValue = statistics.getValueCount() - statistics.getNullCount() > 0;
        if (!hasNonNullValue || statistics.getMaxValue() == null || statistics.getMinValue() == null) {
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        if (!(statistics.getMinValue() instanceof GenericRecord) ||
                !(statistics.getMaxValue() instanceof GenericRecord)) {
            return Domain.all(type);
        }
        return getDomain(colName, type, ((GenericRecord) statistics.getMinValue()).get(0),
                ((GenericRecord) statistics.getMaxValue()).get(0), hasNullValue);
    }

    /**
     * Get a domain for the ranges defined by each pair of elements from {@code minimums} and {@code maximums}.
     * Both arrays must have the same length.
     */
    private static Domain getDomain(String colName, Type type, Object minimum, Object maximum, boolean hasNullValue)
    {
        try {
            if (type.equals(BOOLEAN)) {
                boolean hasTrueValue = (boolean) minimum || (boolean) maximum;
                boolean hasFalseValue = !(boolean) minimum || !(boolean) maximum;
                if (hasTrueValue && hasFalseValue) {
                    return Domain.all(type);
                }
                if (hasTrueValue) {
                    return Domain.create(ValueSet.of(type, true), hasNullValue);
                }
                if (hasFalseValue) {
                    return Domain.create(ValueSet.of(type, false), hasNullValue);
                }
                // No other case, since all null case is handled earlier.
            }

            if ((type.equals(BIGINT) || type.equals(TINYINT) || type.equals(SMALLINT)
                    || type.equals(INTEGER) || type.equals(DATE))) {
                long minValue = TupleDomainParquetPredicate.asLong(minimum);
                long maxValue = TupleDomainParquetPredicate.asLong(maximum);
                if (isStatisticsOverflow(type, minValue, maxValue)) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, minValue, maxValue, hasNullValue);
            }

            if (type.equals(REAL)) {
                Float minValue = (Float) minimum;
                Float maxValue = (Float) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, (long) floatToRawIntBits(minValue), (long) floatToRawIntBits(maxValue), hasNullValue);
            }

            if (type.equals(DOUBLE)) {
                Double minValue = (Double) minimum;
                Double maxValue = (Double) maximum;
                if (minValue.isNaN() || maxValue.isNaN()) {
                    return Domain.create(ValueSet.all(type), hasNullValue);
                }
                return ofMinMax(type, minValue, maxValue, hasNullValue);
            }

            if (type.equals(VarcharType.VARCHAR)) {
                Slice min = Slices.utf8Slice((String) minimum);
                Slice max = Slices.utf8Slice((String) maximum);
                return ofMinMax(type, min, max, hasNullValue);
            }
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
        catch (Exception e) {
            log.warn("failed to create Domain for column: %s which type is: %s", colName, type.toString());
            return Domain.create(ValueSet.all(type), hasNullValue);
        }
    }

    private static Domain ofMinMax(Type type, Object min, Object max, boolean hasNullValue)
    {
        Range range = Range.range(type, min, true, max, true);
        ValueSet vs = ValueSet.ofRanges(ImmutableList.of(range));
        return Domain.create(vs, hasNullValue);
    }
}
