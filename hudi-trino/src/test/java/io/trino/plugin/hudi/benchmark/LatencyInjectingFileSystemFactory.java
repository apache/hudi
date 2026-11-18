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

/**
 * Wraps a local {@link TrinoFileSystemFactory} and charges an object-store-like latency on each request a
 * reader would send to S3: opening a stream (one GET), each positioned or tail read (one ranged GET), a
 * length or existence probe (one HEAD) and each listing call (one LIST). Local disk answers these in
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
        static Latency none()
        {
            return new Latency(0, 0, 0);
        }

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
                if (path.contains("/.hoodie/timeline")) {
                    return "TIMELINE";
                }
                if (path.endsWith("/.hoodie/hoodie.properties")) {
                    return "TABLE_PROPERTIES";
                }
                return path.endsWith("/.hoodie") ? "HOODIE_DIR" : "OTHER_HOODIE";
            }
            if (path.contains(".log.")) {
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
            return new LatencyInjectingInputFile(fileSystem.newInputFile(location));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length)
        {
            return new LatencyInjectingInputFile(fileSystem.newInputFile(location, length));
        }

        @Override
        public TrinoInputFile newInputFile(Location location, long length, Instant lastModified)
        {
            return new LatencyInjectingInputFile(fileSystem.newInputFile(location, length, lastModified));
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

        private LatencyInjectingInputFile(TrinoInputFile inputFile)
        {
            this.inputFile = inputFile;
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
            charge(inputFile.location(), counters.gets, latency.getMillis());
            return inputFile.newStream();
        }

        @Override
        public long length()
                throws IOException
        {
            charge(inputFile.location(), counters.heads, latency.headMillis());
            return inputFile.length();
        }

        @Override
        public Instant lastModified()
                throws IOException
        {
            charge(inputFile.location(), counters.heads, latency.headMillis());
            return inputFile.lastModified();
        }

        @Override
        public boolean exists()
                throws IOException
        {
            charge(inputFile.location(), counters.heads, latency.headMillis());
            return inputFile.exists();
        }

        @Override
        public Location location()
        {
            return inputFile.location();
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
