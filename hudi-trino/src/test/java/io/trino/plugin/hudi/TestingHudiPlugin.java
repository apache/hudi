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

import com.google.common.collect.ImmutableList;
import io.trino.spi.Plugin;
import io.trino.spi.connector.ConnectorFactory;

import java.nio.file.Path;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class TestingHudiPlugin
        implements Plugin
{
    private final Optional<Path> localFileSystemRootPath;

    public TestingHudiPlugin()
    {
        this.localFileSystemRootPath = Optional.empty();
    }

    public TestingHudiPlugin(Path localFileSystemRootPath)
    {
        this.localFileSystemRootPath = Optional.of(requireNonNull(localFileSystemRootPath, "localFileSystemRootPath is null"));
    }

    @Override
    public Iterable<ConnectorFactory> getConnectorFactories()
    {
        if (localFileSystemRootPath.isPresent()) {
            return ImmutableList.of(new TestingHudiConnectorFactory(localFileSystemRootPath.get()));
        }
        return ImmutableList.of(new HudiConnectorFactory());
    }
}
