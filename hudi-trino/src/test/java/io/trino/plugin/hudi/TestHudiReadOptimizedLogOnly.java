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

import io.trino.Session;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiReadOptimizedLogOnly
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setWorkerCount(0)
                .addConnectorProperty("hudi.metadata.cache.enabled", "false")
                .addConnectorProperty("hudi.table-statistics-enabled", "false")
                .setDataLoader(new ResourceHudiTablesInitializer())
                .build();
    }

    @Test
    public void testReadOptimizedSkipsLogOnlyFileGroup()
            throws Exception
    {
        Session session = session(false, false);
        assertThat(computeActual(session, "SELECT count(*) FROM stock_ticks_mor").getOnlyValue()).isEqualTo(99L);
        TrinoFileSystem fileSystem = fileSystem();
        Table table = metastore().getTable("tests", "stock_ticks_mor").orElseThrow();
        var meta = HudiUtil.buildTableMetaClient(fileSystem, "tests.stock_ticks_mor", table.getStorage().getLocation());
        String commit = meta.getActiveTimeline().getCommitsTimeline().filterCompletedInstants().lastInstant().get().requestedTime();
        var files = fileSystem.listFiles(Location.of(table.getStorage().getLocation()));
        Location partition = null;
        while (files.hasNext()) {
            Location file = files.next().location();
            if (file.toString().endsWith(".parquet")) {
                partition = file.parentDirectory();
                break;
            }
        }
        assertThat(partition).isNotNull();
        Location log = partition.appendPath(".logonly_" + commit + ".log.1_0-0-0");
        // Read-optimized planning must skip this file group without opening the synthetic log.
        fileSystem.newOutputFile(log).createOrOverwrite(new byte[100]);
        try {
            assertThat(computeActual(session, "SELECT count(*) FROM stock_ticks_mor").getOnlyValue()).isEqualTo(99L);
        }
        finally {
            fileSystem.deleteFile(log);
        }
    }

    private Session session(boolean metadata, boolean casing)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("hudi", "metadata_enabled", Boolean.toString(metadata))
                .setCatalogSessionProperty("hudi", "resolve_column_name_casing_enabled", Boolean.toString(casing))
                .build();
    }

    private HudiConnector connector()
    {
        return (HudiConnector) getQueryRunner().getCoordinator().getConnector("hudi");
    }

    private TrinoFileSystem fileSystem()
    {
        return connector().getInjector().getInstance(TrinoFileSystemFactory.class).create(ConnectorIdentity.ofUser("test"));
    }

    private HiveMetastore metastore()
    {
        return connector().getInjector().getInstance(HiveMetastoreFactory.class).createMetastore(Optional.empty());
    }
}
