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
import com.google.common.collect.ImmutableSet;
import io.airlift.units.Duration;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hive.util.AsyncQueue;
import io.trino.plugin.hive.util.ThrottledAsyncQueue;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSplit;
import io.trino.spi.connector.ConnectorSplitSource;
import io.trino.spi.connector.Constraint;
import io.trino.spi.connector.DynamicFilterSnapshot;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestHudiSplitSource
{
    private static final SchemaTableName TABLE = new SchemaTableName("tests", "split_source_test");
    private static final Duration TIMEOUT = new Duration(10, SECONDS);
    private static final ConnectorSplit DUMMY_SPLIT = new ConnectorSplit() {};

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();

    @AfterAll
    public void tearDown()
    {
        executor.shutdownNow();
        scheduler.shutdownNow();
    }

    @Test
    public void testNoneDynamicFilterTerminatesSource()
            throws Exception
    {
        AsyncQueue<ConnectorSplit> queue = new ThrottledAsyncQueue<>(100, 100, executor);
        // Loader that stays "in flight": splits are queued but the queue is never finished,
        // so only the early-termination path can make the source report finished
        HudiSplitSource splitSource = new HudiSplitSource(queue, scheduler, (q, errorListener) -> () -> q.offer(DUMMY_SPLIT), TABLE, TIMEOUT);

        CompletableFuture<List<ConnectorSplit>> batch =
                splitSource.getNextBatch(10, new DynamicFilterSnapshot(TupleDomain.none(), true));

        assertThat(batch).isCompletedWithValueMatching(List::isEmpty);
        assertThat(splitSource.isFinished()).isTrue();
    }

    @Test
    public void testNonePredicateYieldsEmptySplitSource()
    {
        // A none() predicate must short-circuit before the metastore is consulted: the split manager would
        // otherwise hand it to computePartitionKeyFilter, which rejects a none() domain
        HudiSplitManager splitManager = new HudiSplitManager(
                (identity, transaction) -> {
                    throw new AssertionError("metastore must not be consulted for a none() predicate");
                },
                executor,
                scheduler);

        for (HudiTableHandle tableHandle : List.of(
                createTableHandle(TupleDomain.none(), TupleDomain.all()),
                createTableHandle(TupleDomain.all(), TupleDomain.none()))) {
            ConnectorSplitSource splitSource = splitManager.getSplits(
                    new HiveTransactionHandle(false), SESSION, tableHandle, ImmutableSet.of(), Constraint.alwaysTrue());

            assertThat(splitSource.isFinished()).isTrue();
            assertThat(splitSource.getNextBatch(10, new DynamicFilterSnapshot(TupleDomain.all(), false)).join()).isEmpty();
        }
    }

    @Test
    public void testLoaderFailureBlocksFinishedAndSurfaces()
            throws Exception
    {
        AsyncQueue<ConnectorSplit> queue = new ThrottledAsyncQueue<>(100, 100, executor);
        HudiSplitSource splitSource = new HudiSplitSource(
                queue,
                scheduler,
                (q, errorListener) -> () -> errorListener.accept(new RuntimeException("boom")),
                TABLE,
                TIMEOUT);
        awaitQueueFinished(queue);

        // The failed load finished the queue, but the source must not claim finished while the
        // failure is undelivered; the next batch is what surfaces it
        assertThat(splitSource.isFinished()).isFalse();
        CompletableFuture<List<ConnectorSplit>> batch =
                splitSource.getNextBatch(10, new DynamicFilterSnapshot(TupleDomain.all(), false));
        assertThatThrownBy(batch::join)
                .hasCauseInstanceOf(TrinoException.class)
                .hasMessageContaining("Failed to generate splits");
    }

    @Test
    public void testCompletedLoaderFinishesNormally()
            throws Exception
    {
        AsyncQueue<ConnectorSplit> queue = new ThrottledAsyncQueue<>(100, 100, executor);
        HudiSplitSource splitSource = new HudiSplitSource(
                queue,
                scheduler,
                (q, errorListener) -> (Runnable) q::finish,
                TABLE,
                TIMEOUT);
        awaitQueueFinished(queue);

        CompletableFuture<List<ConnectorSplit>> batch =
                splitSource.getNextBatch(10, new DynamicFilterSnapshot(TupleDomain.all(), false));
        assertThat(batch.join()).isEmpty();
        assertThat(splitSource.isFinished()).isTrue();
    }

    private static void awaitQueueFinished(AsyncQueue<ConnectorSplit> queue)
            throws InterruptedException
    {
        for (int i = 0; i < 500 && !queue.isFinished(); i++) {
            Thread.sleep(10);
        }
        assertThat(queue.isFinished()).isTrue();
    }

    private static HudiTableHandle createTableHandle(
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates)
    {
        return new HudiTableHandle(
                TABLE.getSchemaName(),
                TABLE.getTableName(),
                "/test/path",
                HoodieTableType.COPY_ON_WRITE,
                ImmutableList.of(),
                ImmutableList.of(),
                partitionPredicates,
                regularPredicates,
                OptionalLong.empty(),
                "",
                "101");
    }
}
