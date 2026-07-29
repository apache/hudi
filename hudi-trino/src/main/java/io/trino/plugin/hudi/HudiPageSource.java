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

import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.spi.Page;
import io.trino.spi.PageBuilder;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.IOException;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.getCausalChain;
import static com.google.common.base.Throwables.throwIfUnchecked;

public class HudiPageSource
        implements ConnectorPageSource
{
    private final HoodieFileGroupReader<IndexedRecord> fileGroupReader;
    // Reads flow through fileGroupReader; pageSource is kept for stats/isBlocked delegation
    private final ConnectorPageSource pageSource;
    private final PageBuilder pageBuilder;
    private final HudiAvroSerializer avroSerializer;
    private final ClosableIterator<IndexedRecord> recordIterator;

    public HudiPageSource(
            ConnectorPageSource pageSource,
            HoodieFileGroupReader<IndexedRecord> fileGroupReader,
            List<HiveColumnHandle> columnHandles,
            PrefilledColumnValues prefilledColumnValues)
    {
        this.pageSource = pageSource;
        this.fileGroupReader = fileGroupReader;
        this.pageBuilder = new PageBuilder(columnHandles.stream().map(HiveColumnHandle::getType).toList());
        this.avroSerializer = new HudiAvroSerializer(columnHandles, prefilledColumnValues);
        try {
            this.recordIterator = fileGroupReader.getClosableIterator();
        }
        catch (Throwable e) {
            // Hudi's log scanning wraps failures in a generic HoodieException ("Exception when
            // reading log file"), which buries connector errors; if the cause chain holds a
            // TrinoException, throw that one instead so its error code and actionable message
            // surface as the query failure.
            Throwable toThrow = getCausalChain(e).stream()
                    .filter(TrinoException.class::isInstance)
                    .findFirst()
                    .orElse(e);
            // getClosableIterator() can fail with checked (IOException) or unchecked
            // (HoodieIOException, NPE/IAE from schema/file validation) exceptions; clean up
            // in all cases so we don't leak the reader/page-source handles.
            try {
                fileGroupReader.close();
            }
            catch (Exception closeException) {
                toThrow.addSuppressed(closeException);
            }
            try {
                pageSource.close();
            }
            catch (Exception closeException) {
                toThrow.addSuppressed(closeException);
            }
            // Preserve the original exception type (RuntimeException/Error) instead of masking it.
            throwIfUnchecked(toThrow);
            throw new RuntimeException("Failed to initialize file group reader!", toThrow);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return pageSource.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return pageSource.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return pageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return !recordIterator.hasNext();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        checkState(pageBuilder.isEmpty(), "PageBuilder is not empty at the beginning of a new page");
        while (recordIterator.hasNext()) {
            avroSerializer.buildRecordInPage(pageBuilder, recordIterator.next());
        }

        Page newPage = pageBuilder.build();
        pageBuilder.reset();
        return SourcePage.create(newPage);
    }

    @Override
    public long getMemoryUsage()
    {
        return pageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        // recordIterator is the outermost wrapper; closing it cascades down through the file
        // group reader to the underlying Trino pageSource, releasing each resource exactly once.
        // Closing fileGroupReader/pageSource here too would double-close the same handles.
        recordIterator.close();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        return pageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return pageSource.getMetrics();
    }
}
