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
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.HiveType;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.storage.TrinoStorageConfiguration;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiEmptyTables
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
    public void testInitializedTableWithoutCommits()
            throws Exception
    {
        String name = "empty_without_commits";
        String basePath = "local:///tests/" + name;
        HoodieTableMetaClient.newTableBuilder()
                .setTableName(name)
                .setTableType(HoodieTableType.COPY_ON_WRITE)
                .setRecordKeyFields("id")
                .initTable(new TrinoStorageConfiguration(fileSystem()), basePath);
        metastore().createTable(Table.builder()
                .setDatabaseName("tests")
                .setTableName(name)
                .setTableType("EXTERNAL_TABLE")
                .setOwner(Optional.of("public"))
                .setDataColumns(List.of(new Column("id", HiveType.HIVE_LONG, Optional.empty(), Map.of())))
                .setParameters(Map.of("EXTERNAL", "TRUE"))
                .withStorage(storage -> storage.setLocation(basePath).setStorageFormat(StorageFormat.create(
                        PARQUET_HIVE_SERDE_CLASS, HUDI_PARQUET_INPUT_FORMAT, MAPRED_PARQUET_OUTPUT_FORMAT_CLASS)))
                .build(), PrincipalPrivileges.NO_PRIVILEGES);

        for (boolean metadata : List.of(false, true)) {
            for (boolean casing : List.of(false, true)) {
                Session session = session(metadata, casing);
                assertThat(computeActual(session, "SELECT count(*) FROM " + name).getOnlyValue()).isEqualTo(0L);
                assertThat(computeActual(session, "SELECT id FROM " + name).getRowCount()).isZero();
            }
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
