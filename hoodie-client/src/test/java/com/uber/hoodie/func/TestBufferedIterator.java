/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.func;

import com.uber.hoodie.common.HoodieTestDataGenerator;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.exception.HoodieException;
import org.apache.avro.generic.IndexedRecord;
import org.apache.commons.io.FileUtils;
import org.apache.spark.util.SizeEstimator;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestBufferedIterator {

    private final HoodieTestDataGenerator hoodieTestDataGenerator = new HoodieTestDataGenerator();
    private final String commitTime = HoodieActiveTimeline.createNewCommitTime();
    private ExecutorService recordReader = null;

    @Before
    public void beforeTest() {
        this.recordReader = Executors.newFixedThreadPool(1);
    }

    @After
    public void afterTest() {
        if (this.recordReader != null) {
            this.recordReader.shutdownNow();
            this.recordReader = null;
        }
    }

    // Test to ensure that we are reading all records from buffered iterator in the same order without any exceptions.
    @Test(timeout = 60000)
    public void testRecordReading() throws IOException, ExecutionException, InterruptedException {
        final int numRecords = 128;
        final List<HoodieRecord> hoodieRecords = hoodieTestDataGenerator.generateInserts(commitTime, numRecords);
        final BufferedIterator bufferedIterator =
            new BufferedIterator(hoodieRecords.iterator(), FileUtils.ONE_KB, HoodieTestDataGenerator.avroSchema);
        Future<Boolean> result =
            recordReader.submit(
                () -> {
                    bufferedIterator.startBuffering();
                    return true;
                }
            );
        final Iterator<HoodieRecord> originalRecordIterator = hoodieRecords.iterator();
        int recordsRead = 0;
        while (bufferedIterator.hasNext()) {
            final HoodieRecord originalRecord = originalRecordIterator.next();
            final Optional<IndexedRecord> originalInsertValue =
                originalRecord.getData().getInsertValue(HoodieTestDataGenerator.avroSchema);
            final BufferedIterator.BufferedIteratorPayload payload = bufferedIterator.next();
            // Ensure that record ordering is guaranteed.
            Assert.assertEquals(originalRecord, payload.record);
            // cached insert value matches the expected insert value.
            Assert.assertEquals(originalInsertValue, payload.insertValue);
            recordsRead++;
        }
        Assert.assertFalse(bufferedIterator.hasNext() || originalRecordIterator.hasNext());
        // all the records should be read successfully.
        Assert.assertEquals(numRecords, recordsRead);
        // should not throw any exceptions.
        Assert.assertTrue(result.get());
    }

    // Test to ensure that record buffering is throttled when we hit memory limit.
    @Test(timeout = 60000)
    public void testMemoryLimitForBuffering() throws IOException, InterruptedException {
        final int numRecords = 128;
        final List<HoodieRecord> hoodieRecords = hoodieTestDataGenerator.generateInserts(commitTime, numRecords);
        // maximum number of records to keep in memory.
        final int recordLimit = 5;
        final long memoryLimitInBytes = recordLimit * SizeEstimator.estimate(hoodieRecords.get(0));
        final BufferedIterator bufferedIterator =
            new BufferedIterator(hoodieRecords.iterator(), memoryLimitInBytes, HoodieTestDataGenerator.avroSchema);
        Future<Boolean> result =
            recordReader.submit(
                () -> {
                    bufferedIterator.startBuffering();
                    return true;
                }
            );
        // waiting for permits to expire.
        while (!isQueueFull(bufferedIterator.rateLimiter)) {
            Thread.sleep(10);
        }
        Assert.assertEquals(0, bufferedIterator.rateLimiter.availablePermits());
        Assert.assertEquals(recordLimit, bufferedIterator.currentRateLimit);
        Assert.assertEquals(recordLimit, bufferedIterator.size());
        Assert.assertEquals(recordLimit - 1, bufferedIterator.samplingRecordCounter.get());

        // try to read 2 records.
        Assert.assertEquals(hoodieRecords.get(0), bufferedIterator.next().record);
        Assert.assertEquals(hoodieRecords.get(1), bufferedIterator.next().record);

        // waiting for permits to expire.
        while (!isQueueFull(bufferedIterator.rateLimiter)) {
            Thread.sleep(10);
        }
        // No change is expected in rate limit or number of buffered records. We only expect buffering thread to read
        // 2 more records into the buffer.
        Assert.assertEquals(0, bufferedIterator.rateLimiter.availablePermits());
        Assert.assertEquals(recordLimit, bufferedIterator.currentRateLimit);
        Assert.assertEquals(recordLimit, bufferedIterator.size());
        Assert.assertEquals(recordLimit - 1 + 2, bufferedIterator.samplingRecordCounter.get());
    }

    // Test to ensure that exception in either buffering thread or BufferedIterator-reader thread is propagated to
    // another thread.
    @Test(timeout = 60000)
    public void testException() throws IOException, InterruptedException {
        final int numRecords = 256;
        final List<HoodieRecord> hoodieRecords = hoodieTestDataGenerator.generateInserts(commitTime, numRecords);
        // buffer memory limit
        final long memoryLimitInBytes = 4 * SizeEstimator.estimate(hoodieRecords.get(0));

        // first let us throw exception from bufferIterator reader and test that buffering thread stops and throws
        // correct exception back.
        BufferedIterator bufferedIterator1 =
            new BufferedIterator(hoodieRecords.iterator(), memoryLimitInBytes, HoodieTestDataGenerator.avroSchema);
        Future<Boolean> result =
            recordReader.submit(
                () -> {
                    bufferedIterator1.startBuffering();
                    return true;
                }
            );
        // waiting for permits to expire.
        while (!isQueueFull(bufferedIterator1.rateLimiter)) {
            Thread.sleep(10);
        }
        // notify buffering thread of an exception and ensure that it exits.
        final Exception e = new Exception("Failing it :)");
        bufferedIterator1.markAsFailed(e);
        try {
            result.get();
            Assert.fail("exception is expected");
        } catch (ExecutionException e1) {
            Assert.assertEquals(HoodieException.class, e1.getCause().getClass());
            Assert.assertEquals(e, e1.getCause().getCause());
        }

        // second let us raise an exception while doing record buffering. this exception should get propagated to
        // buffered iterator reader.
        final RuntimeException expectedException = new RuntimeException("failing record reading");
        final Iterator<HoodieRecord> mockHoodieRecordsIterator = mock(Iterator.class);
        when(mockHoodieRecordsIterator.hasNext()).thenReturn(true);
        when(mockHoodieRecordsIterator.next()).thenThrow(expectedException);
        BufferedIterator bufferedIterator2 =
            new BufferedIterator(mockHoodieRecordsIterator, memoryLimitInBytes, HoodieTestDataGenerator.avroSchema);
        Future<Boolean> result2 =
            recordReader.submit(
                () -> {
                    bufferedIterator2.startBuffering();
                    return true;
                }
            );
        try {
            bufferedIterator2.hasNext();
            Assert.fail("exception is expected");
        } catch (Exception e1) {
            Assert.assertEquals(expectedException, e1.getCause());
        }
        // buffering thread should also have exited. make sure that it is not running.
        try {
            result2.get();
            Assert.fail("exception is expected");
        } catch (ExecutionException e2) {
            Assert.assertEquals(expectedException, e2.getCause());
        }
    }

    private boolean isQueueFull(Semaphore rateLimiter) {
        return (rateLimiter.availablePermits() == 0 && rateLimiter.hasQueuedThreads());
    }
}
