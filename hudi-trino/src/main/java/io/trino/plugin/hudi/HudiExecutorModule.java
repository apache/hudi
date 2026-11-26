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

import com.google.inject.Binder;
import com.google.inject.Key;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import io.trino.metastore.HiveMetastore;
import io.trino.plugin.hive.HiveTransactionHandle;
import io.trino.plugin.hudi.stats.ForHudiTableStatistics;
import io.trino.spi.security.ConnectorIdentity;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.BiFunction;

import static io.airlift.bootstrap.ClosingBinder.closingBinder;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Executors and the transaction-scoped metastore getter that {@link HudiSplitManager} and
 * {@link HudiMetadataFactory} need. Installed by {@link HudiModule} and by Trino's lakehouse
 * connector, which cannot install {@link HudiModule} itself. Requires {@link HudiConfig} and
 * {@link HudiTransactionManager} to be bound.
 */
public class HudiExecutorModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHudiTableStatistics.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHudiSplitManager.class));
        closingBinder(binder).registerExecutor(Key.get(ScheduledExecutorService.class, ForHudiSplitSource.class));
        closingBinder(binder).registerExecutor(Key.get(ExecutorService.class, ForHudiSplitOpen.class));
    }

    @Provides
    @Singleton
    @ForHudiTableStatistics
    public ExecutorService createTableStatisticsExecutor(HudiConfig hudiConfig)
    {
        return newScheduledThreadPool(
                hudiConfig.getTableStatisticsExecutorParallelism(),
                daemonThreadsNamed("hudi-table-statistics-executor-%s"));
    }

    @Provides
    @Singleton
    @ForHudiSplitManager
    public ExecutorService createExecutorService()
    {
        return newCachedThreadPool(daemonThreadsNamed("hudi-split-manager-%s"));
    }

    @Provides
    @Singleton
    @ForHudiSplitSource
    public ScheduledExecutorService createSplitLoaderExecutor(HudiConfig hudiConfig)
    {
        return newScheduledThreadPool(
                hudiConfig.getSplitLoaderParallelism(),
                daemonThreadsNamed("hudi-split-loader-%s"));
    }

    /**
     * Opens merge-on-read splits off the split's thread. Work is rejected when every thread is busy: there is no queue,
     * so a caller can always fall back to doing the work itself.
     */
    @Provides
    @Singleton
    @ForHudiSplitOpen
    public ExecutorService createSplitOpenExecutor(HudiConfig hudiConfig)
    {
        ThreadFactory threadFactory = daemonThreadsNamed("hudi-split-open-%s");
        ClassLoader classLoader = HudiExecutorModule.class.getClassLoader();
        return new ThreadPoolExecutor(
                0,
                hudiConfig.getMorVectorizedReadSplitOpenThreads(),
                30,
                SECONDS,
                new SynchronousQueue<>(),
                runnable -> {
                    Thread thread = threadFactory.newThread(runnable);
                    thread.setContextClassLoader(classLoader);
                    return thread;
                });
    }

    @Provides
    @Singleton
    public BiFunction<ConnectorIdentity, HiveTransactionHandle, HiveMetastore> createHiveMetastoreGetter(HudiTransactionManager transactionManager)
    {
        return (identity, transactionHandle) ->
                transactionManager.get(transactionHandle, identity).getMetastore();
    }
}
