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
import io.trino.plugin.hudi.reader.HudiTrinoReaderContext;
import io.trino.plugin.hudi.util.HudiAvroSerializer;
import io.trino.plugin.hudi.util.SynthesizedColumnHandler;
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

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;

public class HudiPageSource
        implements ConnectorPageSource
{
    ClosableIterator<IndexedRecord> fileGroupReaderIterator;
    ConnectorPageSource pageSource;
    HudiTrinoReaderContext readerContext;
    PageBuilder pageBuilder;
    HudiAvroSerializer avroSerializer;
    List<HiveColumnHandle> columnHandles;

    public HudiPageSource(
            ConnectorPageSource pageSource,
            HoodieFileGroupReader<IndexedRecord> fileGroupReader,
            HudiTrinoReaderContext readerContext,
            List<HiveColumnHandle> columnHandles,
            SynthesizedColumnHandler synthesizedColumnHandler)
    {
        this.pageSource = pageSource;
        this.readerContext = readerContext;
        this.columnHandles = columnHandles;
        this.pageBuilder = new PageBuilder(columnHandles.stream().map(HiveColumnHandle::getType).toList());
        this.avroSerializer = new HudiAvroSerializer(columnHandles, synthesizedColumnHandler);
        try {
            this.fileGroupReaderIterator = fileGroupReader.getClosableIterator();
        }
        catch (IOException e) {
            try {
                fileGroupReader.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            try {
                pageSource.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "Error initializing Hudi file group reader: " + e.getMessage(), e);
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
        return !fileGroupReaderIterator.hasNext();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        while (fileGroupReaderIterator.hasNext()) {
            avroSerializer.buildRecordInPage(pageBuilder, fileGroupReaderIterator.next());
        }

        if (pageBuilder.isEmpty()) {
            return null;
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
        fileGroupReaderIterator.close();
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
