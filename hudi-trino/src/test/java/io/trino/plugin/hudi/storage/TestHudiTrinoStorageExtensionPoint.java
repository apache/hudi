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

import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.memory.MemoryFileSystem;
import org.apache.hudi.common.util.HoodieStorageUtils;
import org.apache.hudi.storage.HoodieStorage;
import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.StoragePath;
import org.junit.jupiter.api.Test;

import java.io.FileNotFoundException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestHudiTrinoStorageExtensionPoint
{
    private static final StoragePath PATH = new StoragePath("memory:///warehouse/table");

    @Test
    void testStorageResolvesFromConfiguration()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        StorageConfiguration<?> conf = new TrinoStorageConfiguration(fileSystem);

        HoodieStorage storage = HoodieStorageUtils.getStorage(PATH, conf);

        assertThat(storage).isInstanceOf(HudiTrinoStorage.class);
        assertThat(storage.getConf()).isSameAs(conf);
    }

    @Test
    void testResolvedStorageIsUsable()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        HoodieStorage storage = HoodieStorageUtils.getStorage(PATH, new TrinoStorageConfiguration(fileSystem));

        assertThatThrownBy(() -> storage.getPathInfo(new StoragePath(PATH, "absent")))
                .isInstanceOf(FileNotFoundException.class);
    }

    @Test
    void testConfigurationWithoutFileSystemFailsClearly()
    {
        assertThatThrownBy(() -> HoodieStorageUtils.getStorage(PATH, new TrinoStorageConfiguration()))
                .rootCause()
                .hasMessageContaining("carries no file system");
    }

    @Test
    void testDerivedConfigurationsKeepTheFileSystem()
    {
        TrinoFileSystem fileSystem = new MemoryFileSystem();
        TrinoStorageConfiguration conf = new TrinoStorageConfiguration(fileSystem);

        assertThat(HoodieStorageUtils.getStorage(PATH, conf.newInstance()))
                .isInstanceOf(HudiTrinoStorage.class);
        assertThat(HoodieStorageUtils.getStorage(PATH, conf.getInline()))
                .isInstanceOf(HudiTrinoStorage.class);
    }
}
