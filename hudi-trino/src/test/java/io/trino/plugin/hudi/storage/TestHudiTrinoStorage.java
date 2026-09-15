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
package io.trino.plugin.hudi.storage;

import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

class TestHudiTrinoStorage
{
    @Test
    void testConvertToPathInfo()
    {
        FileEntry fileEntry = new FileEntry(
                Location.of("memory:///table/data.parquet"),
                42,
                Instant.ofEpochMilli(1234567890123L),
                Optional.empty());

        StoragePathInfo pathInfo = HudiTrinoStorage.convertToPathInfo(fileEntry);

        assertThat(pathInfo.getPath()).isEqualTo(new StoragePath("memory:///table/data.parquet"));
        assertThat(pathInfo.getLength()).isEqualTo(42);
        assertThat(pathInfo.isFile()).isTrue();
        assertThat(pathInfo.getBlockReplication()).isEqualTo((short) 0);
        assertThat(pathInfo.getBlockSize()).isEqualTo(42);
        assertThat(pathInfo.getModificationTime()).isEqualTo(1234567890123L);
    }

    @Test
    void testGetPathInfoForFile()
            throws IOException
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        writeFile(fileSystem, "memory:///table/data.parquet", 42);
        HudiTrinoStorage storage = new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration());

        StoragePathInfo pathInfo = storage.getPathInfo(new StoragePath("memory:///table/data.parquet"));

        assertThat(pathInfo.getLength()).isEqualTo(42);
        assertThat(pathInfo.isFile()).isTrue();
        assertThat(pathInfo.getBlockSize()).isEqualTo(42);
        assertThat(pathInfo.getModificationTime()).isGreaterThan(0);
    }

    @Test
    void testGetPathInfoForDirectory()
            throws IOException
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        writeFile(fileSystem, "memory:///table/data.parquet", 42);
        HudiTrinoStorage storage = new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration());

        StoragePathInfo pathInfo = storage.getPathInfo(new StoragePath("memory:///table"));

        assertThat(pathInfo.isDirectory()).isTrue();
        assertThat(pathInfo.getLength()).isEqualTo(0);
        assertThat(pathInfo.getBlockSize()).isEqualTo(0);
    }

    @Test
    void testListFiles()
            throws IOException
    {
        HudiTrinoStorage storage = createStorageWithFiles();

        List<StoragePathInfo> entries = storage.listFiles(new StoragePath("memory:///table"));

        assertThat(entries).hasSize(3);
        assertThat(entries.get(0).getPath()).isEqualTo(new StoragePath("memory:///table/a.parquet"));
        assertThat(entries.get(1).getPath()).isEqualTo(new StoragePath("memory:///table/b.parquet"));
        assertThat(entries.get(2).getPath()).isEqualTo(new StoragePath("memory:///table/nested/c.parquet"));
        assertThat(entries.get(0).getLength()).isEqualTo(10);
        assertThat(entries.get(1).getLength()).isEqualTo(20);
        assertThat(entries.get(2).getLength()).isEqualTo(30);
        for (StoragePathInfo entry : entries) {
            assertThat(entry.getBlockSize()).isEqualTo(entry.getLength());
        }
    }

    @Test
    void testListDirectEntries()
            throws IOException
    {
        HudiTrinoStorage storage = createStorageWithFiles();

        List<StoragePathInfo> entries = storage.listDirectEntries(new StoragePath("memory:///table"));

        assertThat(entries).hasSize(2);
        assertThat(entries.get(0).getPath()).isEqualTo(new StoragePath("memory:///table/a.parquet"));
        assertThat(entries.get(1).getPath()).isEqualTo(new StoragePath("memory:///table/b.parquet"));
        for (StoragePathInfo entry : entries) {
            assertThat(entry.getBlockSize()).isEqualTo(entry.getLength());
        }
    }

    @Test
    void testListDirectEntriesWithFilter()
            throws IOException
    {
        HudiTrinoStorage storage = createStorageWithFiles();

        List<StoragePathInfo> entries = storage.listDirectEntries(
                new StoragePath("memory:///table"),
                path -> path.getName().equals("b.parquet"));

        assertThat(entries).hasSize(1);
        assertThat(entries.get(0).getPath()).isEqualTo(new StoragePath("memory:///table/b.parquet"));
        assertThat(entries.get(0).getLength()).isEqualTo(20);
        assertThat(entries.get(0).getBlockSize()).isEqualTo(20);
    }

    private static HudiTrinoStorage createStorageWithFiles()
            throws IOException
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        writeFile(fileSystem, "memory:///table/a.parquet", 10);
        writeFile(fileSystem, "memory:///table/b.parquet", 20);
        writeFile(fileSystem, "memory:///table/nested/c.parquet", 30);
        return new HudiTrinoStorage(fileSystem, new TrinoStorageConfiguration());
    }

    private static void writeFile(TrinoFileSystem fileSystem, String location, int length)
            throws IOException
    {
        fileSystem.newOutputFile(Location.of(location)).createOrOverwrite(new byte[length]);
    }
}
