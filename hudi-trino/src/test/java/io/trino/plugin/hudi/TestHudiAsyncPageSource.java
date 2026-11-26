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

import io.trino.spi.Page;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.FixedPageSource;
import io.trino.spi.connector.MemoryContext;
import io.trino.spi.connector.SourcePage;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static io.trino.plugin.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHudiAsyncPageSource
{
    @Test
    void testPagesComeOnceTheOpenCompletes()
            throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger closes = new AtomicInteger();
            HudiAsyncPageSource pageSource = new HudiAsyncPageSource(_ -> {
                await(release);
                return closeCounting(closes, new Page(3));
            }, executor);
            assertThat(pageSource.isBlocked()).isNotDone();
            assertThat(pageSource.getNextSourcePage()).isNull();
            assertThat(pageSource.isFinished()).isFalse();
            release.countDown();
            pageSource.isBlocked().get(10, TimeUnit.SECONDS);
            SourcePage page = pageSource.getNextSourcePage();
            assertThat(page.getPositionCount()).isEqualTo(3);
            assertThat(pageSource.getNextSourcePage()).isNull();
            assertThat(pageSource.isFinished()).isTrue();
            pageSource.close();
            assertThat(closes.get()).isEqualTo(1);
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testFailedOpenThrowsItsException()
            throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            TrinoException failure = new TrinoException(HUDI_CANNOT_OPEN_SPLIT, "log files unreadable");
            HudiAsyncPageSource pageSource = new HudiAsyncPageSource(_ -> {
                throw failure;
            }, executor);
            pageSource.isBlocked().get(10, TimeUnit.SECONDS);
            assertThat(pageSource.isFinished()).isFalse();
            assertThatThrownBy(pageSource::getNextSourcePage).isSameAs(failure);
            pageSource.close();
        }
        finally {
            executor.shutdownNow();
        }
    }

    @Test
    void testCloseBeforeTheOpenCompletesClosesWhatItOpens()
            throws Exception
    {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            CountDownLatch release = new CountDownLatch(1);
            AtomicInteger closes = new AtomicInteger();
            AtomicBoolean sawClosed = new AtomicBoolean();
            HudiAsyncPageSource pageSource = new HudiAsyncPageSource(closed -> {
                await(release);
                // An open can stop early once its split is closed
                sawClosed.set(closed.getAsBoolean());
                return closeCounting(closes, new Page(1));
            }, executor);
            pageSource.close();
            assertThat(closes.get()).isZero();
            release.countDown();
            executor.shutdown();
            assertThat(executor.awaitTermination(10, TimeUnit.SECONDS)).isTrue();
            assertThat(sawClosed.get()).isTrue();
            assertThat(closes.get()).isEqualTo(1);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static FixedPageSource closeCounting(AtomicInteger closes, Page page)
    {
        return new FixedPageSource(List.of(page), MemoryContext.NO_LIMIT)
        {
            @Override
            public void close()
            {
                closes.incrementAndGet();
                super.close();
            }
        };
    }

    private static void await(CountDownLatch latch)
    {
        try {
            latch.await();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
    }
}
