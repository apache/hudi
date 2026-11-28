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

import io.airlift.log.Logger;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;
import io.trino.spi.metrics.Metrics;

import java.io.IOException;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executor;
import java.util.function.BooleanSupplier;
import java.util.function.Function;

import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.util.Objects.requireNonNull;

/**
 * A page source opened on an executor: blocked until the open completes, then the opened page source. The open, such as
 * loading a split's log records, does not hold the split's thread, and a failed open throws from
 * {@link #getNextSourcePage} the exception the open threw.
 */
public class HudiAsyncPageSource
        implements ConnectorPageSource
{
    private static final Logger log = Logger.get(HudiAsyncPageSource.class);

    private final CompletableFuture<ConnectorPageSource> open;
    // Completes when the open completes, either way; a failure is thrown by getNextSourcePage
    private final CompletableFuture<?> opened;
    private ConnectorPageSource delegate;
    private volatile boolean closed;

    /**
     * @param open opens the page source; it is handed whether this page source is closed, so it can stop early
     */
    public HudiAsyncPageSource(Function<BooleanSupplier, ConnectorPageSource> open, Executor executor)
    {
        requireNonNull(open, "open is null");
        this.open = CompletableFuture.supplyAsync(() -> open.apply(() -> closed), requireNonNull(executor, "executor is null"));
        this.opened = this.open.handle((_, _) -> null);
    }

    /**
     * The opened page source, or null while the open runs. Throws what the open threw.
     */
    private ConnectorPageSource delegate()
    {
        if (delegate == null && open.isDone()) {
            try {
                delegate = open.join();
            }
            catch (CompletionException e) {
                throwIfUnchecked(e.getCause());
                throw new RuntimeException(e.getCause());
            }
        }
        return delegate;
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate == null ? 0 : delegate.getCompletedBytes();
    }

    @Override
    public OptionalLong getCompletedPositions()
    {
        return delegate == null ? OptionalLong.empty() : delegate.getCompletedPositions();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate == null ? 0 : delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        // An open that failed is not finished: getNextSourcePage throws its exception
        return delegate != null && delegate.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        ConnectorPageSource pageSource = delegate();
        return pageSource == null ? null : pageSource.getNextSourcePage();
    }

    @Override
    public long getMemoryUsage()
    {
        return delegate == null ? 0 : delegate.getMemoryUsage();
    }

    @Override
    public CompletableFuture<?> isBlocked()
    {
        if (!open.isDone()) {
            return opened;
        }
        ConnectorPageSource pageSource = delegate;
        return pageSource == null ? NOT_BLOCKED : pageSource.isBlocked();
    }

    @Override
    public Metrics getMetrics()
    {
        return delegate == null ? Metrics.EMPTY : delegate.getMetrics();
    }

    @Override
    public void close()
            throws IOException
    {
        if (closed) {
            return;
        }
        closed = true;
        if (delegate != null) {
            delegate.close();
            return;
        }
        // An open still running stops early when it can and closes what it opens once it completes; a failed open
        // closes its own sources
        open.whenComplete((pageSource, _) -> {
            if (pageSource != null) {
                try {
                    pageSource.close();
                }
                catch (IOException | RuntimeException e) {
                    log.debug(e, "Failed to close a page source opened after its split was closed");
                }
            }
        });
    }
}
