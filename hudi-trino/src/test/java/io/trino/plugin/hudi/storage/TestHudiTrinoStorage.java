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
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Optional;

import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHudiTrinoStorage
{
    private static final StoragePath EXTENSION_POINT_PATH = new StoragePath("memory:///warehouse/table");

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

    @Test
    void testStorageResolvesFromConfiguration()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        StorageConfiguration<?> conf = new TrinoStorageConfiguration(fileSystem);

        HoodieStorage storage = HoodieStorageUtils.getStorage(EXTENSION_POINT_PATH, conf);

        assertThat(storage).isInstanceOf(HudiTrinoStorage.class);
        assertThat(storage.getConf()).isSameAs(conf);
    }

    @Test
    void testResolvedStorageCarriesPassedFileSystem()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();

        HoodieStorage storage = HoodieStorageUtils.getStorage(
                EXTENSION_POINT_PATH, new TrinoStorageConfiguration(fileSystem));

        assertThat(((HudiTrinoStorage) storage).getFileSystem()).isSameAs(fileSystem);
    }

    @Test
    void testConfigurationWithoutFileSystemFailsClearly()
    {
        assertThatThrownBy(() -> HoodieStorageUtils.getStorage(EXTENSION_POINT_PATH, new TrinoStorageConfiguration()))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("carries no file system");
    }

    @Test
    void testForeignConfigurationFailsClearly()
    {
        HadoopStorageConfiguration conf = new HadoopStorageConfiguration(new Configuration());
        conf.set(HOODIE_STORAGE_CLASS.key(), HudiTrinoStorage.class.getName());

        assertThatThrownBy(() -> HoodieStorageUtils.getStorage(EXTENSION_POINT_PATH, conf))
                .rootCause()
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("is not a TrinoStorageConfiguration");
    }

    @Test
    void testDerivedConfigurationsKeepTheFileSystem()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        TrinoStorageConfiguration conf = new TrinoStorageConfiguration(fileSystem);

        assertThat(HoodieStorageUtils.getStorage(EXTENSION_POINT_PATH, conf.newInstance()))
                .isInstanceOf(HudiTrinoStorage.class);
        assertThat(HoodieStorageUtils.getStorage(EXTENSION_POINT_PATH, conf.getInline()))
                .isInstanceOf(HudiTrinoStorage.class);
        assertThat(((TrinoStorageConfiguration) conf.newInstance()).getFileSystem()).containsSame(fileSystem);
        assertThat(((TrinoStorageConfiguration) conf.getInline()).getFileSystem()).containsSame(fileSystem);
    }

    @Test
    void testInitTableWritesThroughExtensionPoint()
            throws IOException
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();

        HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
                .setTableName("t")
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .initTable(new TrinoStorageConfiguration(fileSystem), EXTENSION_POINT_PATH);
        metaClient.getActiveTimeline().createNewInstant(
                metaClient.createNewInstant(HoodieInstant.State.REQUESTED, "commit", "001"));

        assertThat(fileSystem.newInputFile(
                Location.of(EXTENSION_POINT_PATH + "/.hoodie/hoodie.properties")).exists()).isTrue();
        assertThat(fileSystem.newInputFile(
                Location.of(EXTENSION_POINT_PATH + "/.hoodie/timeline/001.commit.requested")).exists()).isTrue();
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
