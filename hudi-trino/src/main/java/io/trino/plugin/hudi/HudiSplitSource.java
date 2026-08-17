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

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import io.trino.metastore.Partition;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HivePartitionKey;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.HiveUtil;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.plugin.hudi.query.HudiDirectoryLister;
import io.trino.plugin.hudi.query.HudiSnapshotDirectoryLister;
import io.trino.plugin.hudi.split.HudiBackgroundSplitLoader;
import io.trino.plugin.hudi.split.HudiSplitWeightProvider;
import io.trino.plugin.hudi.split.SizeBasedSplitWeightProvider;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.Domain;
import io.trino.spi.predicate.NullableValue;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.metadata.NativeTableMetadataFactory;
import org.apache.hudi.common.util.Lazy;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.toCompletableFuture;
import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static io.trino.plugin.hudi.HudiSessionProperties.getMinimumAssignedSplitWeight;
import static io.trino.plugin.hudi.HudiSessionProperties.getStandardSplitWeightSize;
import static io.trino.plugin.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static io.trino.plugin.hudi.HudiSessionProperties.isSizeBasedSplitWeightsEnabled;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitSource.class);

    private final AsyncQueue<ConnectorSplit> queue;
    private final ScheduledFuture splitLoaderFuture;
    private final AtomicReference<TrinoException> trinoException = new AtomicReference<>();
    private final AtomicBoolean finished = new AtomicBoolean();
    private final long dynamicFilteringWaitTimeoutMillis;

    public HudiSplitSource(
            ConnectorSession session,
            HudiTableHandle tableHandle,
            ExecutorService executor,
            ScheduledExecutorService splitLoaderExecutorService,
            int maxSplitsPerSecond,
            int maxOutstandingSplits,
            Lazy<Map<String, Partition>> lazyPartitions,
            Duration dynamicFilteringWaitTimeoutMillis)
    {
        this(
                new ThrottledAsyncQueue<>(maxSplitsPerSecond, maxOutstandingSplits, executor),
                splitLoaderExecutorService,
                (queue, errorListener) -> {
                    boolean enableMetadataTable = isHudiMetadataTableEnabled(session);
                    Lazy<HoodieTableMetadata> lazyTableMetadata = Lazy.lazily(() -> {
                        HoodieTimer timer = HoodieTimer.start();
                        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                                .enable(enableMetadataTable)
                                .build();
                        HoodieTableMetaClient metaClient = tableHandle.getMetaClient();
                        HoodieEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorage().getConf());

                        // Defer to the native factory, which creates a HoodieBackedTableMetadata when the
                        // metadata table is enabled and initialized, and falls back to FileSystemBackedTableMetadata
                        // otherwise.
                        HoodieTableMetadata tableMetadata = NativeTableMetadataFactory.getInstance().create(
                                engineContext, metaClient.getStorage(), metadataConfig, metaClient.getBasePath().toString(), true);
                        log.info("Loaded table metadata for table: %s in %s ms", tableHandle.getSchemaTableName(), timer.endTimer());
                        return tableMetadata;
                    });

                    HudiDirectoryLister hudiDirectoryLister = new HudiSnapshotDirectoryLister(
                            session,
                            tableHandle,
                            enableMetadataTable,
                            lazyTableMetadata);

                    return new HudiBackgroundSplitLoader(
                            session,
                            tableHandle,
                            hudiDirectoryLister,
                            queue,
                            executor,
                            createSplitWeightProvider(session),
                            lazyPartitions,
                            enableMetadataTable,
                            lazyTableMetadata,
                            errorListener);
                },
                tableHandle.getSchemaTableName(),
                dynamicFilteringWaitTimeoutMillis);
    }

    // Visible for tests: lets a test drive the split loader directly while keeping the
    // error-listener wiring (set trinoException, then finish the queue -- isFinished
    // relies on that order) identical to production.
    HudiSplitSource(
            AsyncQueue<ConnectorSplit> queue,
            ScheduledExecutorService splitLoaderExecutorService,
            BiFunction<AsyncQueue<ConnectorSplit>, Consumer<Throwable>, Runnable> splitLoaderFactory,
            SchemaTableName tableName,
            Duration dynamicFilteringWaitTimeoutMillis)
    {
        this.queue = queue;
        Runnable splitLoader = splitLoaderFactory.apply(queue, throwable -> {
            trinoException.compareAndSet(null, new TrinoException(HUDI_CANNOT_OPEN_SPLIT,
                    "Failed to generate splits for " + tableName, throwable));
            queue.finish();
        });
        this.splitLoaderFuture = splitLoaderExecutorService.schedule(splitLoader, 0, TimeUnit.MILLISECONDS);
        this.dynamicFilteringWaitTimeoutMillis = dynamicFilteringWaitTimeoutMillis.toMillis();
    }

    @Override
    public CompletableFuture<List<ConnectorSplit>> getNextBatch(int maxSize, DynamicFilterSnapshot dynamicFilterSnapshot)
    {
        TupleDomain<HiveColumnHandle> dynamicFilterPredicate =
                dynamicFilterSnapshot.currentPredicate().transformKeys(HiveColumnHandle.class::cast);

        if (dynamicFilterPredicate.isNone()) {
            close();
            // The queue may still hold undrained splits, so queue.isFinished() would never turn true here
            finished.set(true);
            return completedFuture(ImmutableList.of());
        }

        Throwable throwable = trinoException.get();
        if (throwable != null) {
            return CompletableFuture.failedFuture(throwable);
        }

        return toCompletableFuture(Futures.transform(
                queue.getBatchAsync(maxSize),
                splits -> splits.stream()
                        .filter(split -> partitionMatchesPredicate((HudiSplit) split, dynamicFilterPredicate))
                        .collect(toImmutableList()),
                directExecutor()));
    }

    @Override
    public void close()
    {
        queue.finish();
    }

    @Override
    public boolean isFinished()
    {
        // The failure callback sets trinoException before finishing the queue, so once the queue
        // reports finished the exception (if any) is visible here. Claiming finished while an
        // exception is pending would let the engine stop polling and end the scan silently;
        // reporting unfinished instead makes the next getNextBatch surface the failure.
        return finished.get() || (splitLoaderFuture.isDone() && queue.isFinished() && trinoException.get() == null);
    }

    @Override
    public long getRequestedDynamicFilterWaitTimeoutMillis()
    {
        return dynamicFilteringWaitTimeoutMillis;
    }

    public static HudiSplitWeightProvider createSplitWeightProvider(ConnectorSession session)
    {
        if (isSizeBasedSplitWeightsEnabled(session)) {
            DataSize standardSplitWeightSize = getStandardSplitWeightSize(session);
            double minimumAssignedSplitWeight = getMinimumAssignedSplitWeight(session);
            return new SizeBasedSplitWeightProvider(minimumAssignedSplitWeight, standardSplitWeightSize);
        }
        return HudiSplitWeightProvider.uniformStandardWeightProvider();
    }

    static boolean partitionMatchesPredicate(
            HudiSplit split,
            TupleDomain<HiveColumnHandle> dynamicFilterPredicate)
    {
        if (dynamicFilterPredicate.isNone()) {
            return false;
        }

        // Pre-process the filter predicate to get a map of relevant partition domains keyed by partition column name
        Map<String, Map.Entry<HiveColumnHandle, Domain>> filterPartitionDomains = new HashMap<>();
        if (dynamicFilterPredicate.getDomains().isPresent()) {
            for (Map.Entry<HiveColumnHandle, Domain> entry : dynamicFilterPredicate.getDomains().get().entrySet()) {
                HiveColumnHandle column = entry.getKey();
                if (column.isPartitionKey()) {
                    filterPartitionDomains.put(column.getName(), entry);
                }
            }
        }

        // Match each partition key from the split against the pre-processed filter domains
        for (HivePartitionKey splitPartitionKey : split.getPartitionKeys()) {
            Map.Entry<HiveColumnHandle, Domain> filterInfo = filterPartitionDomains.get(splitPartitionKey.name());

            if (filterInfo == null) {
                // filterInfo is null, the partition key is not constrained by the filter
                continue;
            }

            HiveColumnHandle filterColumnHandle = filterInfo.getKey();
            Domain filterDomain = filterInfo.getValue();

            NullableValue value = HiveUtil.getPrefilledColumnValue(
                    filterColumnHandle,
                    splitPartitionKey,
                    null, OptionalInt.empty(), 0, 0, "");

            // Split does not match this filter condition
            if (!filterDomain.includesNullableValue(value.getValue())) {
                return false;
            }
        }
        return true;
    }
}
