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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hive.metastore.file.FileHiveMetastoreConfig;
import io.trino.plugin.hudi.HudiConnector;
import io.trino.spi.Plugin;
import io.trino.spi.connector.Connector;
import io.trino.spi.connector.ConnectorContext;
import io.trino.spi.connector.ConnectorFactory;
import io.trino.spi.security.PrincipalType;
import io.trino.testing.DistributedQueryRunner;

import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;

import static com.google.inject.multibindings.MapBinder.newMapBinder;
import static io.airlift.configuration.ConfigBinder.configBinder;
import static io.trino.plugin.hudi.HudiConnectorFactory.createConnector;
import static io.trino.testing.TestingSession.testSessionBuilder;

/**
 * A local query runner for the MOR read benchmarks: the {@code hudi} catalog reads a local directory through
 * {@link LatencyInjectingFileSystemFactory}, so every storage request can be charged an S3-like latency.
 */
final class BenchmarkHudiQueryRunner
{
    static final String CATALOG = "hudi";
    static final String SCHEMA = "tpcds";

    private BenchmarkHudiQueryRunner() {}

    static DistributedQueryRunner create(
            int workerCount,
            LatencyInjectingFileSystemFactory.Latency latency,
            LatencyInjectingFileSystemFactory.Counters counters,
            Map<String, String> connectorProperties,
            TpcdsMorTablesInitializer tables)
            throws Exception
    {
        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(testSessionBuilder()
                        .setCatalog(CATALOG)
                        .setSchema(SCHEMA)
                        .build())
                .setWorkerCount(workerCount)
                .build();
        try {
            Path dataRoot = queryRunner.getCoordinator().getBaseDataDir().resolve("hudi_data");
            dataRoot.toFile().mkdirs();
            queryRunner.installPlugin(new BenchmarkHudiPlugin(dataRoot, latency, counters));
            queryRunner.createCatalog(CATALOG, "hudi", ImmutableMap.<String, String>builder()
                    .put("hudi.metadata-enabled", "false")
                    .put("hudi.table-statistics-enabled", "false")
                    .put("fs.cache.enabled", "false")
                    .putAll(connectorProperties)
                    .buildOrThrow());
            ((HudiConnector) queryRunner.getCoordinator().getConnector(CATALOG)).getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty())
                    .createDatabase(Database.builder()
                            .setDatabaseName(SCHEMA)
                            .setOwnerName(Optional.of("public"))
                            .setOwnerType(Optional.of(PrincipalType.ROLE))
                            .build());
            tables.initializeTables(queryRunner, Location.of("local:///"), SCHEMA);
            return queryRunner;
        }
        catch (Throwable e) {
            queryRunner.close();
            throw e;
        }
    }

    private static final class BenchmarkHudiPlugin
            implements Plugin
    {
        private final Path dataRoot;
        private final LatencyInjectingFileSystemFactory.Latency latency;
        private final LatencyInjectingFileSystemFactory.Counters counters;

        private BenchmarkHudiPlugin(Path dataRoot, LatencyInjectingFileSystemFactory.Latency latency, LatencyInjectingFileSystemFactory.Counters counters)
        {
            this.dataRoot = dataRoot;
            this.latency = latency;
            this.counters = counters;
        }

        @Override
        public Iterable<ConnectorFactory> getConnectorFactories()
        {
            return ImmutableList.of(new ConnectorFactory()
            {
                @Override
                public String getName()
                {
                    return "hudi";
                }

                @Override
                public Connector create(String catalogName, Map<String, String> config, ConnectorContext context)
                {
                    Map<String, String> fullConfig = ImmutableMap.<String, String>builder()
                            .putAll(config)
                            .put("bootstrap.quiet", "true")
                            .put("hive.metastore", "file")
                            .buildOrThrow();
                    TrinoFileSystemFactory local = new LocalFileSystemFactory(dataRoot);
                    return createConnector(catalogName, fullConfig, context, Optional.of(binder -> {
                        newMapBinder(binder, String.class, TrinoFileSystemFactory.class)
                                .addBinding("local").toInstance(new LatencyInjectingFileSystemFactory(local, latency, counters));
                        configBinder(binder).bindConfigDefaults(FileHiveMetastoreConfig.class, metastoreConfig -> metastoreConfig.setCatalogDirectory("local:///managed/"));
                    }));
                }
            });
        }
    }
}
