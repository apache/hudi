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
package io.trino.plugin.hudi.benchmark;

import io.trino.filesystem.FileIterator;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.TrinoOutputFile;
import io.trino.spi.security.ConnectorIdentity;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Instant;
import java.util.Comparator;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.fs.FSUtils.isLogFile;

/**
 * Wraps a local {@link TrinoFileSystemFactory} and charges an object-store-like latency on each request a
 * reader would send to S3: the first read of a stream and each read after a seek S3 cannot serve by skipping (one
 * GET), each positioned or tail read (one ranged GET), an
 * existence probe or a length or modification time not yet known (one HEAD) and each listing call (one LIST).
 * As Trino's {@code S3InputFile} does, a length or modification time passed to {@code newInputFile} or returned
 * by an earlier HEAD is answered without another request. Local disk answers these in
 * microseconds, which hides any per-split metadata read; the injected latency makes those reads cost what
 * they cost on S3, so a local run shows their effect on wall time and on dynamic-filter timing.
 *
 * <p>The counters are environment-independent: they count requests regardless of the latency setting, so
 * a change that removes per-split metadata reads shows up as a lower count even with zero latency.
 */
final class LatencyInjectingFileSystemFactory
        implements TrinoFileSystemFactory
{
    private final TrinoFileSystemFactory delegate;
    private final Latency latency;
    private final Counters counters;

    LatencyInjectingFileSystemFactory(TrinoFileSystemFactory delegate, Latency latency, Counters counters)
    {
        this.delegate = requireNonNull(delegate, "delegate is null");
        this.latency = requireNonNull(latency, "latency is null");
        this.counters = requireNonNull(counters, "counters is null");
    }

    @Override
    public TrinoFileSystem create(ConnectorIdentity identity)
    {
        return new LatencyInjectingFileSystem(delegate.create(identity));
    }

    /**
     * Per-request latencies in milliseconds. Defaults approximate same-region S3 from EC2: a first-byte GET of
     * ~20ms, a HEAD of ~15ms, a LIST page of ~40ms.
     */
    record Latency(long getMillis, long headMillis, long listMillis)
    {
        static Latency fromSystemProperties()
        {
            long scale = Long.getLong("bench.s3.latency.enabled", 1) == 0 ? 0 : 1;
            return new Latency(
                    scale * Long.getLong("bench.s3.getMillis", 20),
                    scale * Long.getLong("bench.s3.headMillis", 15),
                    scale * Long.getLong("bench.s3.listMillis", 40));
        }
    }

    /** Request counts since the last {@link #reset()}, in total and per request type and Hudi file type. */
    static final class Counters
    {
        final AtomicLong gets = new AtomicLong();
        final AtomicLong heads = new AtomicLong();
        final AtomicLong lists = new AtomicLong();
        private final Map<String, AtomicLong> byKind = new ConcurrentHashMap<>();
        private final Map<String, String> samples = new ConcurrentHashMap<>();

        void reset()
        {
            gets.set(0);
            heads.set(0);
            lists.set(0);
            byKind.clear();
            samples.clear();
        }

        void record(AtomicLong counter, Location location)
        {
            counter.incrementAndGet();
            String request = counter == gets ? "GET" : counter == heads ? "HEAD" : "LIST";
            String kind = request + ":" + fileType(location);
            byKind.computeIfAbsent(kind, key -> new AtomicLong()).incrementAndGet();
            samples.putIfAbsent(kind, location.toString());
        }

        /** One example location per request and file type, to see which files a request type hits. */
        String samples()
        {
            return samples.entrySet().stream()
                    .sorted(Map.Entry.comparingByKey())
                    .map(entry -> entry.getKey() + " -> " + entry.getValue())
                    .collect(Collectors.joining("\n"));
        }

        /** Per request type and file type, e.g. {@code HEAD:LOG=96}, largest first. */
        String breakdown()
        {
            return byKind.entrySet().stream()
                    .sorted(Map.Entry.<String, AtomicLong>comparingByValue(Comparator.comparingLong(AtomicLong::get)).reversed())
                    .map(entry -> entry.getKey() + "=" + entry.getValue().get())
                    .collect(Collectors.joining(" "));
        }

        private static String fileType(Location location)
        {
            String path = location.toString();
            if (path.contains("/.hoodie")) {
                if (path.contains("/.hoodie/metadata")) {
                    return "METADATA_TABLE";
                }
                if (path.contains("/.hoodie/.index_defs")) {
                    return "INDEX_DEFINITION";
                }
                if (path.contains("/.hoodie/timeline")) {
                    return "TIMELINE";
                }
                if (path.endsWith("/.hoodie/hoodie.properties")) {
                    return "TABLE_PROPERTIES";
                }
                return path.endsWith("/.hoodie") ? "HOODIE_DIR" : "OTHER_HOODIE";
            }
            if (isLogFile(location.fileName())) {
                return "LOG";
            }
            if (path.endsWith(".parquet")) {
                return "BASE";
            }
            if (path.contains(".hoodie_partition_metadata")) {
                return "PARTITION_METADATA";
            }
            return path.contains("=") ? "PARTITION_DIR" : "TABLE_DIR";
        }

        @Override
        public String toString()
        {
            return "GET=" + gets.get() + " HEAD=" + heads.get() + " LIST=" + lists.get();
        }
    }

    private void charge(Location location, AtomicLong counter, long millis)
    {
        // The file metastore keeps its table definitions on the same filesystem; on a cluster the metastore
        // is a separate service (Glue, HMS), so its files are not charged as table storage requests.
        String path = location.toString();
        if (path.contains("/.trino") || path.contains("/managed/")) {
            return;
        }
        counters.record(counter, location);
        if (millis > 0) {
            try {
                TimeUnit.MILLISECONDS.sleep(millis);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private final class LatencyInjectingFileSystem
            implements TrinoFileSystem
    {
        private final TrinoFileSystem fileSystem;

        private LatencyInjectingFileSystem(TrinoFileSystem fileSystem)
        {
            this.fileSystem = fileSystem;
        }

        @Override
        public TrinoInputFile newInputFile(Location location)
        {
            return new LatencyInjectingInputFile(fileSystem.newInputFile(location), null, null);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return new LatencyInjectingInputFile(fileSystem.newInputFile(location, length), length, null);
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return new LatencyInjectingInputFile(fileSystem.newInputFile(location, length, lastModified), length, lastModified);
        }

        @Override
        public TrinoOutputFile newOutputFile(Location location)
        {
            return fileSystem.newOutputFile(location);
        }

        @Override
        public void deleteFile(Location location)
                throws IOException
        {
            fileSystem.deleteFile(location);
        }

        @Override
        public void deleteDirectory(Location location)
                throws IOException
        {
            fileSystem.deleteDirectory(location);
        }

        @Override
        public void renameFile(Location source, Location target)
                throws IOException
        {
            fileSystem.renameFile(source, target);
        }

        @Override
        public FileIterator listFiles(Location location)
                throws IOException
        {
            charge(location, counters.lists, latency.listMillis());
            return fileSystem.listFiles(location);
        }

        @Override
        public Optional<Boolean> directoryExists(Location location)
                throws IOException
        {
            charge(location, counters.lists, latency.listMillis());
            return fileSystem.directoryExists(location);
        }

        @Override
        public void createDirectory(Location location)
                throws IOException
        {
            fileSystem.createDirectory(location);
        }

        @Override
        public void renameDirectory(Location source, Location target)
                throws IOException
        {
            fileSystem.renameDirectory(source, target);
        }

        @Override
        public Set<Location> listDirectories(Location location)
                throws IOException
        {
            charge(location, counters.lists, latency.listMillis());
            return fileSystem.listDirectories(location);
        }

        @Override
        public Optional<Location> createTemporaryDirectory(Location targetPath, String temporaryPrefix, String relativePrefix)
                throws IOException
        {
            return fileSystem.createTemporaryDirectory(targetPath, temporaryPrefix, relativePrefix);
        }
    }

    private final class LatencyInjectingInputFile
            implements TrinoInputFile
    {
        private final TrinoInputFile inputFile;
        // Null until passed to newInputFile or filled by a HEAD, as S3InputFile keeps them
        private Long length;
        private Instant lastModified;

        private LatencyInjectingInputFile(TrinoInputFile inputFile, Long length, Instant lastModified)
        {
            this.inputFile = inputFile;
            this.length = length;
            this.lastModified = lastModified;
        }

        @Override
        public TrinoInput newInput()
                throws IOException
        {
            return new LatencyInjectingInput(inputFile.location(), inputFile.newInput());
        }

        @Override
        public TrinoInputStream newStream()
                throws IOException
        {
            return new LatencyInjectingInputStream(inputFile.location(), inputFile.newStream());
        }

        @Override
        public long length()
                throws IOException
        {
            if (length == null && !head()) {
                throw new FileNotFoundException(inputFile.location().toString());
            }
            return length;
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            if (lastModified == null && !head()) {
                throw new FileNotFoundException(inputFile.location().toString());
            }
            return lastModified;
        }

        @Override
        public boolean exists()
                throws IOException
        {
            return head();
        }

        /** One HEAD, which fills whichever of length and modification time is not yet known. */
        private boolean head()
                throws IOException
        {
            charge(inputFile.location(), counters.heads, latency.headMillis());
            if (!inputFile.exists()) {
                return false;
            }
            if (length == null) {
                length = inputFile.length();
            }
            if (lastModified == null) {
                lastModified = inputFile.lastModified();
            }
            return true;
        }

        @Override
        public Location location()
        {
            return inputFile.location();
        }
    }

    /**
     * Charges GETs as Trino's {@code S3InputStream} sends them: none when the stream is opened, one on the first read,
     * and one on each read that follows a backward seek or a forward seek longer than {@link #MAX_SKIP_BYTES}, which
     * S3 serves by reopening the object at the new position rather than skipping on the open connection.
     * {@code S3InputStream} also skips up to the bytes its connection has buffered; that is taken as zero here.
     */
    private final class LatencyInjectingInputStream
            extends TrinoInputStream
    {
        private static final long MAX_SKIP_BYTES = 1024 * 1024;

        private final Location location;
        private final TrinoInputStream stream;
        private boolean connected;
        private long streamPosition;
        private long nextReadPosition;

        private LatencyInjectingInputStream(Location location, TrinoInputStream stream)
        {
            this.location = location;
            this.stream = stream;
        }

        @Override
        public long getPosition()
                throws IOException
        {
            return stream.getPosition();
        }

        @Override
        public void seek(long position)
                throws IOException
        {
            stream.seek(position);
            nextReadPosition = position;
        }

        @Override
        public int read()
                throws IOException
        {
            connect();
            int value = stream.read();
            if (value >= 0) {
                advance(1);
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length)
                throws IOException
        {
            connect();
            int read = stream.read(buffer, offset, length);
            if (read > 0) {
                advance(read);
            }
            return read;
        }

        @Override
        public long skip(long n)
                throws IOException
        {
            long skipped = stream.skip(n);
            nextReadPosition += skipped;
            return skipped;
        }

        @Override
        public int available()
                throws IOException
        {
            return stream.available();
        }

        @Override
        public void close()
                throws IOException
        {
            stream.close();
        }

        private void connect()
        {
            long gap = nextReadPosition - streamPosition;
            if (connected && gap >= 0 && gap <= MAX_SKIP_BYTES) {
                streamPosition = nextReadPosition;
                return;
            }
            charge(location, counters.gets, latency.getMillis());
            connected = true;
            streamPosition = nextReadPosition;
        }

        private void advance(int bytes)
        {
            nextReadPosition += bytes;
            streamPosition = nextReadPosition;
        }
    }

    private final class LatencyInjectingInput
            implements TrinoInput
    {
        private final Location location;
        private final TrinoInput input;

        private LatencyInjectingInput(Location location, TrinoInput input)
        {
            this.location = location;
            this.input = input;
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int length)
                throws IOException
        {
            charge(location, counters.gets, latency.getMillis());
            input.readFully(position, buffer, bufferOffset, length);
        }

        @Override
        public int readTail(byte[] buffer, int bufferOffset, int maxLength)
                throws IOException
        {
            charge(location, counters.gets, latency.getMillis());
            return input.readTail(buffer, bufferOffset, maxLength);
        }

        @Override
        public void close()
                throws IOException
        {
            input.close();
        }
    }
}
