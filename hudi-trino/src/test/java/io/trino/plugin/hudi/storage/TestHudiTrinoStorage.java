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

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.FileEntry;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.filesystem.memory.MemoryFileSystem;
import io.trino.testing.TestingConnectorSession;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.io.storage.TestHoodieStorageBase;
import org.apache.hudi.metadata.FileSystemBackedTableMetadata;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.apache.hudi.storage.StoragePathInfo;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.file.Path;
import java.time.Instant;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static org.apache.hudi.common.config.HoodieStorageConfig.HOODIE_STORAGE_CLASS;
import static org.apache.hudi.storage.HoodieInstantWriter.convertByteArrayToWriter;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHudiTrinoStorage
        extends TestHoodieStorageBase
{
    private static final StoragePath EXTENSION_POINT_PATH = new StoragePath("memory:///warehouse/table");
    private static final byte[] EMPTY_BYTES = {};

    @Override
    protected HoodieStorage getStorage(Object fileSystem, Object config)
    {
        return new HudiTrinoStorage((TrinoFileSystem) fileSystem, (TrinoStorageConfiguration) config);
    }

    @Override
    protected TrinoFileSystem getFileSystem(Object config)
    {
        return new LocalFileSystemFactory(tempDir.getParent()).create(TestingConnectorSession.SESSION);
    }

    @Override
    protected TrinoStorageConfiguration getConf()
    {
        return new TrinoStorageConfiguration();
    }

    @Override
    protected String getTempDir()
    {
        return "local:///" + tempDir.getFileName();
    }

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

        assertThat(entries).hasSize(3);
        assertThat(entries.get(0).getPath()).isEqualTo(new StoragePath("memory:///table/a.parquet"));
        assertThat(entries.get(1).getPath()).isEqualTo(new StoragePath("memory:///table/b.parquet"));
        assertThat(entries.get(2).getPath()).isEqualTo(new StoragePath("memory:///table/nested"));
        assertThat(entries.get(2).isDirectory()).isTrue();
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
    void testInitTableWritesThroughExtensionPoint(@TempDir Path directory)
            throws IOException
    {
        // Initializing a timeline requires a real empty directory, which MemoryFileSystem cannot store.
        TrinoFileSystem fileSystem = new LocalFileSystemFactory(directory).create(TestingConnectorSession.SESSION);
        StoragePath tablePath = new StoragePath("local:///warehouse/table");

        HoodieTableMetaClient metaClient = HoodieTableMetaClient.newTableBuilder()
                .setTableName("t")
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .initTable(new TrinoStorageConfiguration(fileSystem), tablePath);
        metaClient.getActiveTimeline().createNewInstant(
                metaClient.createNewInstant(HoodieInstant.State.REQUESTED, "commit", "001"));

        assertThat(fileSystem.newInputFile(
                Location.of(tablePath + "/.hoodie/hoodie.properties")).exists()).isTrue();
        assertThat(fileSystem.newInputFile(
                Location.of(tablePath + "/.hoodie/timeline/001.commit.requested")).exists()).isTrue();
    }

    @Override
    @Test
    public void testGetUri()
    {
        // The external connector uses a placeholder URI; paths retain their own scheme and authority.
        URI uri = getStorage().getUri();
        assertThat(uri).isEqualTo(URI.create("file"));
        for (String location : List.of("local:///data/file", "memory:///data/file", "s3://bucket/data/file")) {
            StoragePath path = new StoragePath(location);
            assertThat(path.makeQualified(uri)).isEqualTo(path);
        }
    }

    // This test is overridden since HudiTrinoStorage does not support globEntries,
    // as it is not used in the Trino Hudi connector
    @Override
    @Test
    public void testListing()
            throws IOException
    {
        HoodieStorage storage = getStorage();
        // Full list:
        // w/1.file
        // w/2.file
        // x/1.file
        // x/2.file
        // x/y/1.file
        // x/y/2.file
        // x/z/1.file
        // x/z/2.file
        prepareFilesOnStorage(storage);

        validatePathInfoList(
                storage.listDirectEntries(new StoragePath(getTempDir(), "x")),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("x/1.file", false))
                        .add(getStoragePathInfo("x/2.file", false))
                        .add(getStoragePathInfo("x/y", true))
                        .add(getStoragePathInfo("x/z", true))
                        .build());

        validatePathInfoList(
                storage.listFiles(new StoragePath(getTempDir(), "x")),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("x/1.file", false))
                        .add(getStoragePathInfo("x/2.file", false))
                        .add(getStoragePathInfo("x/y/1.file", false))
                        .add(getStoragePathInfo("x/y/2.file", false))
                        .add(getStoragePathInfo("x/z/1.file", false))
                        .add(getStoragePathInfo("x/z/2.file", false))
                        .build());

        validatePathInfoList(
                storage.listDirectEntries(
                        new StoragePath(getTempDir(), "x"), e -> e.getName().contains("2")),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("x/2.file", false))
                        .build());

        validatePathInfoList(
                storage.listDirectEntries(ImmutableList.<StoragePath>builder()
                        .add(new StoragePath(getTempDir(), "w"))
                        .add(new StoragePath(getTempDir(), "x/z"))
                        .build()),
                ImmutableList.<StoragePathInfo>builder()
                        .add(getStoragePathInfo("w/1.file", false))
                        .add(getStoragePathInfo("w/2.file", false))
                        .add(getStoragePathInfo("x/z/1.file", false))
                        .add(getStoragePathInfo("x/z/2.file", false))
                        .build());

        assertThatThrownBy(
                () -> storage.listDirectEntries(new StoragePath(getTempDir(), "*")))
                .isInstanceOf(FileNotFoundException.class);

        // HudiTrinoStorage does not support globEntries, as it is not used
        // in the Trino Hudi connector
        assertThatThrownBy(
                () -> storage.globEntries(new StoragePath(getTempDir(), "x/*/1.file")))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    // This test is overridden since HudiTrinoStorage always returns true for deletion,
    // because TrinoFileSystem does not indicate whether the file to delete exists or not
    @Override
    @Test
    public void testDelete()
            throws IOException
    {
        HoodieStorage storage = getStorage();

        StoragePath path = new StoragePath(getTempDir(), "testDelete/1.file");
        assertThat(storage.exists(path)).isFalse();
        storage.create(path).close();
        assertThat(storage.exists(path)).isTrue();

        assertThat(storage.deleteFile(path)).isTrue();
        assertThat(storage.exists(path)).isFalse();
        // TrinoFileSystem does not indicate whether the file to delete exists or not
        assertThat(storage.deleteFile(path)).isTrue();

        StoragePath path2 = new StoragePath(getTempDir(), "testDelete/2");
        assertThat(storage.exists(path2)).isFalse();
        assertThat(storage.createDirectory(path2)).isTrue();
        assertThat(storage.exists(path2)).isTrue();

        assertThat(storage.deleteDirectory(path2)).isTrue();
        assertThat(storage.exists(path2)).isFalse();
        // TrinoFileSystem does not indicate whether the directory to delete exists or not
        assertThat(storage.deleteDirectory(path2)).isTrue();
    }

    // This test is overridden because TrinoFileSystem does not overwrite the file with
    // #create so the test logic has to be adapted (not that this behavior does not affect
    // product code logic in Trino Hudi connector as it's read-only)
    @Override
    @Test
    public void testCreateWriteAndRead()
            throws IOException
    {
        HoodieStorage storage = getStorage();

        StoragePath path = new StoragePath(getTempDir(), "testCreateAppendAndRead/1.file");
        assertThat(storage.exists(path)).isFalse();
        storage.create(path).close();
        validatePathInfo(storage, path, EMPTY_BYTES, false);
        storage.deleteFile(path);

        byte[] data = {2, 42, 49, (byte) 158, (byte) 233, 66, 9};

        try (OutputStream stream = storage.create(path)) {
            stream.write(data);
            stream.flush();
        }
        validatePathInfo(storage, path, data, false);

        assertThatThrownBy(() -> storage.create(path, false))
                .isInstanceOf(IOException.class);
        validatePathInfo(storage, path, data, false);

        assertThatThrownBy(() -> storage.create(path, false))
                .isInstanceOf(IOException.class);
        validatePathInfo(storage, path, data, false);

        StoragePath path2 = new StoragePath(getTempDir(), "testCreateAppendAndRead/2.file");
        assertThat(storage.exists(path2)).isFalse();
        assertThat(storage.createNewFile(path2)).isTrue();
        validatePathInfo(storage, path2, EMPTY_BYTES, false);
        assertThat(storage.createNewFile(path2)).isFalse();

        StoragePath path3 = new StoragePath(getTempDir(), "testCreateAppendAndRead/3.file");
        assertThat(storage.exists(path3)).isFalse();
        storage.createImmutableFileInPath(path3, Option.of(convertByteArrayToWriter(data)));
        validatePathInfo(storage, path3, data, false);

        StoragePath path4 = new StoragePath(getTempDir(), "testCreateAppendAndRead/4");
        assertThat(storage.exists(path4)).isFalse();
        assertThat(storage.createDirectory(path4)).isTrue();
        validatePathInfo(storage, path4, EMPTY_BYTES, true);
        assertThat(storage.createDirectory(path4)).isTrue();
    }

    @Test
    void testListEmptyDirectoryOnHierarchicalFileSystem()
            throws IOException
    {
        HoodieStorage storage = getStorage();

        // Create an empty directory on hierarchical filesystem (LocalFileSystem)
        StoragePath emptyDir = new StoragePath(getTempDir(), "empty_directory");
        assertThat(storage.createDirectory(emptyDir)).isTrue();
        assertThat(storage.exists(emptyDir)).isTrue();

        // On hierarchical filesystem, listing empty directory should return empty list
        List<StoragePathInfo> entries = storage.listDirectEntries(emptyDir);
        assertThat(entries).isEmpty();

        List<StoragePathInfo> files = storage.listFiles(emptyDir);
        assertThat(files).isEmpty();
    }

    @Test
    void testListEmptyDirectoryOnNonHierarchicalFileSystem()
            throws IOException
    {
        // Test HudiTrinoStorage behavior on a non-hierarchical filesystem (like S3)
        MemoryFileSystem memoryFileSystem = new MemoryFileSystem();
        try (HoodieStorage storage = new HudiTrinoStorage(memoryFileSystem, new TrinoStorageConfiguration())) {
            // On non-hierarchical filesystem, directories are virtual and only exist if they contain files.
            // An "empty directory" doesn't exist, so listing should throw FileNotFoundException.
            StoragePath emptyDir = new StoragePath("memory:///empty_directory");

            assertThatThrownBy(() -> storage.listDirectEntries(emptyDir))
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining("does not exist");

            assertThatThrownBy(() -> storage.listDirectEntries(emptyDir, _ -> true))
                    .isInstanceOf(FileNotFoundException.class)
                    .hasMessageContaining("does not exist");
        }
    }

    @Test
    void testListNonExistentDirectory()
            throws IOException
    {
        HoodieStorage storage = getStorage();

        // Try to list a non-existent directory - should throw FileNotFoundException
        StoragePath nonExistentDir = new StoragePath(getTempDir(), "does_not_exist");
        assertThat(storage.exists(nonExistentDir)).isFalse();

        assertThatThrownBy(() -> storage.listDirectEntries(nonExistentDir))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("does not exist");

        assertThatThrownBy(() -> storage.listDirectEntries(nonExistentDir, _ -> true))
                .isInstanceOf(FileNotFoundException.class)
                .hasMessageContaining("does not exist");
    }

    @Test
    void testListDirectEntriesWithDirectoryFilter()
            throws IOException
    {
        HoodieStorage storage = getStorage();
        prepareFilesOnStorage(storage);

        validatePathInfoList(
                storage.listDirectEntries(new StoragePath(getTempDir(), "x"), path -> path.getName().equals("y")),
                List.of(getStoragePathInfo("x/y", true)));
    }

    @Test
    void testPartitionDiscoveryUsingFileSystemMetadata()
            throws Exception
    {
        try (HoodieStorage storage = new HudiTrinoStorage(new MemoryFileSystem(), new TrinoStorageConfiguration())) {
            StoragePath basePath = new StoragePath("memory:///table");
            for (String partition : List.of("dt=2026-11-01", "dt=2026-11-02/hh=00")) {
                storage.create(new StoragePath(basePath, partition + "/.hoodie_partition_metadata")).close();
            }

            try (FileSystemBackedTableMetadata metadata = new FileSystemBackedTableMetadata(
                    new HoodieLocalEngineContext(storage.getConf()), new HoodieTableConfig(), storage, basePath.toString())) {
                assertThat(metadata.getAllPartitionPaths()).containsExactlyInAnyOrder("dt=2026-11-01", "dt=2026-11-02/hh=00");
            }
        }
    }

    @Test
    void testListingIncludesEmptyChildDirectories()
            throws IOException
    {
        HoodieStorage storage = getStorage();
        StoragePath parent = new StoragePath(getTempDir(), "parent");
        StoragePath child = new StoragePath(parent, "empty");
        storage.createDirectory(child);
        assertThat(storage.listDirectEntries(parent))
                .singleElement().satisfies(entry -> {
                    assertThat(entry.getPath()).isEqualTo(child);
                    assertThat(entry.isDirectory()).isTrue();
                });
        assertThat(storage.listDirectEntries(parent, path -> path.getName().equals("empty")))
                .singleElement().satisfies(entry -> assertThat(entry.getPath()).isEqualTo(child));
        assertThat(storage.listDirectEntries(parent, path -> false)).isEmpty();
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

    private HoodieStorage getStorage()
    {
        TrinoStorageConfiguration config = getConf();
        return getStorage(getFileSystem(config), config);
    }

    private StoragePathInfo getStoragePathInfo(String subPath, boolean isDirectory)
    {
        return new StoragePathInfo(
                new StoragePath(getTempDir(), subPath),
                0,
                isDirectory,
                (short) 1,
                1000000L,
                10L);
    }

    private static void validatePathInfo(
            HoodieStorage storage,
            StoragePath path,
            byte[] data,
            boolean isDirectory)
            throws IOException
    {
        assertThat(storage.exists(path)).isTrue();
        StoragePathInfo pathInfo = storage.getPathInfo(path);
        assertThat(pathInfo.getPath()).isEqualTo(path);
        assertThat(pathInfo.isDirectory()).isEqualTo(isDirectory);
        assertThat(pathInfo.isFile()).isEqualTo(!isDirectory);
        if (!isDirectory) {
            assertThat(pathInfo.getLength()).isEqualTo(data.length);
            try (InputStream stream = storage.open(path)) {
                assertThat(stream.readAllBytes()).isEqualTo(data);
            }
            assertThat(pathInfo.getModificationTime()).isGreaterThan(0);
        }
    }

    private static void validatePathInfoList(
            List<StoragePathInfo> actual,
            List<StoragePathInfo> expected)
    {
        assertThat(actual).hasSize(expected.size());
        List<StoragePathInfo> sortedExpected = expected.stream()
                .sorted(Comparator.comparing(StoragePathInfo::getPath))
                .collect(toImmutableList());
        List<StoragePathInfo> sortedActual = actual.stream()
                .sorted(Comparator.comparing(StoragePathInfo::getPath))
                .collect(toImmutableList());
        for (int i = 0; i < expected.size(); i++) {
            // We cannot use StoragePathInfo#equals as that only compares the path
            assertThat(sortedActual.get(i).getPath()).isEqualTo(sortedExpected.get(i).getPath());
            assertThat(sortedActual.get(i).isDirectory()).isEqualTo(sortedExpected.get(i).isDirectory());
            assertThat(sortedActual.get(i).isFile()).isEqualTo(sortedExpected.get(i).isFile());
            if (sortedExpected.get(i).isFile()) {
                assertThat(sortedActual.get(i).getLength()).isEqualTo(sortedExpected.get(i).getLength());
            }
            assertThat(sortedActual.get(i).getModificationTime()).isGreaterThan(0);
        }
    }

    private void prepareFilesOnStorage(HoodieStorage storage)
            throws IOException
    {
        String directory = getTempDir();
        for (String relativePath : RELATIVE_FILE_PATHS) {
            storage.create(new StoragePath(directory, relativePath)).close();
        }
    }
}
