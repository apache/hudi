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

import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hudi.common.table.read.HoodieFileGroupReader;
import org.apache.hudi.common.util.collection.ClosableIterator;

import java.io.IOException;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;

/**
 * Page source for {@code SELECT count(*)} on MOR file groups (with log files). Counts records
 * emitted by {@link HoodieFileGroupReader} (so log inserts/deletes are reflected) and emits
 * empty {@link SourcePage}s carrying just the position count, in batches of {@link #BATCH_SIZE}.
 *
 * <p>The COW base-file-only count(*) path bypasses this class entirely - it returns the underlying
 * Parquet page source directly, which already emits empty pages with correct row counts.
 */
public class HudiCountPageSource
        implements ConnectorPageSource
{
    private static final int BATCH_SIZE = 1024;

    private final HoodieFileGroupReader<IndexedRecord> fileGroupReader;
    private final ClosableIterator<IndexedRecord> iterator;
    private boolean done;

    public HudiCountPageSource(HoodieFileGroupReader<IndexedRecord> fileGroupReader)
    {
        this.fileGroupReader = fileGroupReader;
        try {
            this.iterator = fileGroupReader.getClosableIterator();
        }
        catch (IOException e) {
            try {
                fileGroupReader.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw new TrinoException(HUDI_CANNOT_OPEN_SPLIT,
                    "Error initializing Hudi file group reader for count(*): " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isFinished()
    {
        return done;
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        if (done) {
            return null;
        }
        int count = 0;
        while (count < BATCH_SIZE && iterator.hasNext()) {
            iterator.next();
            count++;
        }
        if (count == 0) {
            done = true;
            return null;
        }
        return SourcePage.create(count);
    }

    @Override
    public long getCompletedBytes()
    {
        return 0;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public long getMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
            throws IOException
    {
        iterator.close();
    }
}
