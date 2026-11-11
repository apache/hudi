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

import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.memory.MemoryInputFile;
import io.trino.plugin.hudi.io.InlineTrinoInputFile;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.inline.InLineFSUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.lang.reflect.Proxy;

import static io.trino.plugin.hudi.HudiPageSourceProvider.newLogInputFile;
import static org.assertj.core.api.Assertions.assertThat;

class TestHudiLogInputFile
{
    private static final String LOG_FILE_CONTENT = "0123456789ABCDEFGHIJ";

    @ParameterizedTest
    @ValueSource(strings = {
            "s3://bucket/warehouse/tbl/dt=2024-01-01/.f1_20240101.log.1_0-1-1",
            "s3a://bucket/warehouse/tbl/.f1_20240101.log.1_0-1-1",
            "gs://bucket/warehouse/tbl/.f1_20240101.log.1_0-1-1",
            "abfs://container@account.dfs.core.windows.net/warehouse/tbl/.f1_20240101.log.1_0-1-1",
            "abfss://container@account.dfs.core.windows.net/warehouse/tbl/.f1_20240101.log.1_0-1-1",
            "local:///tmp/warehouse/tbl/.f1_20240101.log.1_0-1-1",
            "file:///tmp/warehouse/tbl/.f1_20240101.log.1_0-1-1"})
    void testInlineBlockOpensTheLogFile(String logFile)
            throws IOException
    {
        // Hudi normalizes the log path (local:///tmp becomes local:/tmp) before it builds the block's inline path,
        // so the block must open the log file at that same location
        StoragePath logPath = new StoragePath(logFile);
        StoragePath inlinePath = InLineFSUtils.getInlineFilePath(logPath, logPath.toUri().getScheme(), 5, 10);

        TrinoInputFile inputFile = newLogInputFile(fileSystemOf(logPath.toString()), inlinePath.toString());

        assertThat(inputFile).isInstanceOf(InlineTrinoInputFile.class);
        assertThat(inputFile.location()).isEqualTo(Location.of(logPath.toString()));
        assertThat(inputFile.length()).isEqualTo(10);
        try (TrinoInput input = inputFile.newInput()) {
            assertThat(input.readTail(10).toStringUtf8()).isEqualTo("56789ABCDE");
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {
            "s3://bucket/warehouse/tbl/.f1_20240101.log.1_0-1-1",
            "s3://bucket/warehouse/tbl/f1_0-1-1_20240101.log.parquet"})
    void testLogFileOpensAsIs(String logFile)
            throws IOException
    {
        TrinoInputFile inputFile = newLogInputFile(fileSystemOf(logFile), logFile);

        assertThat(inputFile).isNotInstanceOf(InlineTrinoInputFile.class);
        assertThat(inputFile.location()).isEqualTo(Location.of(logFile));
        assertThat(inputFile.length()).isEqualTo(LOG_FILE_CONTENT.length());
    }

    /**
     * A file system holding only the given file; opening any other location fails the test.
     */
    private static TrinoFileSystem fileSystemOf(String file)
    {
        Location location = Location.of(file);
        return (TrinoFileSystem) Proxy.newProxyInstance(
                TrinoFileSystem.class.getClassLoader(),
                new Class<?>[] {TrinoFileSystem.class},
                (proxy, method, args) -> {
                    if (method.getName().equals("newInputFile") && args.length == 1) {
                        assertThat(args[0]).isEqualTo(location);
                        return new MemoryInputFile(location, Slices.utf8Slice(LOG_FILE_CONTENT));
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }
}
