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
import com.google.common.collect.ImmutableMap;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Column;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.PrincipalPrivileges;
import io.trino.metastore.StorageFormat;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.testing.CommitTimeOrderingHudiTablesInitializer;
import io.trino.plugin.hudi.testing.CompositeHudiTablesInitializer;
import io.trino.plugin.hudi.testing.HudiTablesInitializer;
import io.trino.spi.security.ConnectorIdentity;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.hive.formats.HiveClassNames.HUDI_PARQUET_INPUT_FORMAT;
import static io.trino.hive.formats.HiveClassNames.MAPRED_PARQUET_OUTPUT_FORMAT_CLASS;
import static io.trino.hive.formats.HiveClassNames.PARQUET_HIVE_SERDE_CLASS;
import static io.trino.metastore.HiveType.HIVE_LONG;
import static io.trino.metastore.HiveType.HIVE_STRING;
import static io.trino.plugin.hive.TableType.EXTERNAL_TABLE;
import static io.trino.plugin.hudi.testing.TypeInfoHelper.charHiveType;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Column listings (information_schema over a schema) type each table from its Hudi table schema, which
 * means reading every listed table's Hudi metadata. A table whose metadata cannot be read must not fail the listing
 * of the others: it is listed with its metastore column types, while queries naming it still fail.
 */
public class TestHudiListingWithUnreadableTable
        extends AbstractTestQueryFramework
{
    private static final String UNREADABLE_TABLE = "unreadable_mor";

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader(new CompositeHudiTablesInitializer(
                        new CommitTimeOrderingHudiTablesInitializer(),
                        new UnreadableHudiTableInitializer()))
                .build();
    }

    @Test
    public void testListingFallsBackToMetastoreTypesForUnreadableTable()
    {
        Map<String, ImmutableMap<String, String>> columnTypes = computeActual(
                "SELECT table_name, column_name, data_type FROM information_schema.columns WHERE table_schema = '" + schema() + "'")
                .getMaterializedRows().stream()
                .collect(groupingBy(
                        row -> (String) row.getField(0),
                        toImmutableMap(row -> (String) row.getField(1), row -> (String) row.getField(2))));

        // The readable table is typed from its Hudi table schema, where code is a string
        assertThat(columnTypes.get(CommitTimeOrderingHudiTablesInitializer.TABLE_NAME)).containsEntry("code", "varchar");
        assertThat(columnTypes.get(CommitTimeOrderingHudiTablesInitializer.RT_TABLE_NAME)).containsEntry("code", "varchar");
        // The unreadable one keeps the metastore's types
        assertThat(columnTypes.get(UNREADABLE_TABLE)).containsExactlyInAnyOrderEntriesOf(ImmutableMap.of(
                "key", "varchar",
                "code", "char(5)",
                "ts", "bigint"));
    }

    @Test
    public void testSingleTablePathsFailForUnreadableTable()
    {
        String error = ".*Location of table .*" + UNREADABLE_TABLE + " does not contain Hudi table metadata.*";
        assertQueryFails("SHOW CREATE TABLE " + UNREADABLE_TABLE, error);
        assertQueryFails("SELECT * FROM " + UNREADABLE_TABLE, error);
        assertQueryFails("DESCRIBE " + UNREADABLE_TABLE, error);
        // information_schema filtered to this one table is answered by the engine through the table handle, not the
        // listing, and the engine drops a table whose metadata fails from the result instead of failing the query
        MaterializedResult filtered = computeActual("SELECT column_name, data_type FROM information_schema.columns"
                + " WHERE table_schema = '" + schema() + "' AND table_name = '" + UNREADABLE_TABLE + "'");
        assertThat(filtered.getMaterializedRows()).isEmpty();
    }

    private String schema()
    {
        return getSession().getSchema().orElseThrow();
    }

    /**
     * Registers a Hudi table in the metastore whose location holds a stray file but no {@code .hoodie} directory, as a
     * table whose Hudi metadata was deleted or never written.
     */
    private static class UnreadableHudiTableInitializer
            implements HudiTablesInitializer
    {
        @Override
        public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
                throws Exception
        {
            HudiConnector connector = (HudiConnector) queryRunner.getCoordinator().getConnector("hudi");
            TrinoFileSystem fileSystem = connector.getInjector()
                    .getInstance(TrinoFileSystemFactory.class)
                    .create(ConnectorIdentity.ofUser("test"));
            HiveMetastore metastore = connector.getInjector()
                    .getInstance(HiveMetastoreFactory.class)
                    .createMetastore(Optional.empty());

            Location tableLocation = externalLocation.appendPath(UNREADABLE_TABLE);
            try (OutputStream out = fileSystem.newOutputFile(tableLocation.appendPath("stray.txt")).create()) {
                out.write("not a Hudi table".getBytes(UTF_8));
            }

            Table table = Table.builder()
                    .setDatabaseName(schemaName)
                    .setTableName(UNREADABLE_TABLE)
                    .setTableType(EXTERNAL_TABLE.name())
                    .setOwner(Optional.of("public"))
                    // Hand-written: there is no Hudi schema to derive them from, and code is char(5) to tell the fallback types apart
                    .setDataColumns(ImmutableList.of(
                            new Column("key", HIVE_STRING, Optional.empty(), Map.of()),
                            new Column("code", charHiveType(5), Optional.empty(), Map.of()),
                            new Column("ts", HIVE_LONG, Optional.empty(), Map.of())))
                    .setParameters(ImmutableMap.of("EXTERNAL", "TRUE"))
                    .withStorage(storage -> storage
                            .setStorageFormat(StorageFormat.create(PARQUET_HIVE_SERDE_CLASS, HUDI_PARQUET_INPUT_FORMAT, MAPRED_PARQUET_OUTPUT_FORMAT_CLASS))
                            .setLocation(tableLocation.toString()))
                    .build();
            metastore.createTable(table, PrincipalPrivileges.NO_PRIVILEGES);
        }
    }
}
