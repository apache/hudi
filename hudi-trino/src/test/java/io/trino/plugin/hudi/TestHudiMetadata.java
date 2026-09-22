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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.filesystem.local.LocalFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.Table;
import io.trino.metastore.TableAlreadyExistsException;
import io.trino.spi.connector.ColumnMetadata;
import io.trino.spi.connector.ConnectorTableMetadata;
import io.trino.spi.connector.SaveMode;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.PrincipalType;
import io.trino.spi.type.TypeManager;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

import static com.google.common.util.concurrent.MoreExecutors.newDirectExecutorService;
import static io.trino.spi.type.BigintType.BIGINT;
import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHudiMetadata
{
    @TempDir
    private Path temporaryDirectory;

    @Test
    void testCreateTableRaceDoesNotDeleteWinningTableMetadata()
    {
        CreateRace race = createRace(UnaryOperator.identity(), false);

        assertThatThrownBy(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .isInstanceOf(TableAlreadyExistsException.class);

        assertThat(race.winningTable().get()).isNotNull();
        assertThat(metadataExists(race, race.winningTable().get()))
                .isTrue();
    }

    @Test
    void testCreateTableRaceCleansMetadataWhenWinnerUsesDifferentLocation()
    {
        CreateRace race = createRace(table -> Table.builder(table)
                .withStorage(storage -> storage.setLocation("local:///other_table"))
                .build(), false);

        assertThatThrownBy(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .isInstanceOf(TableAlreadyExistsException.class);

        assertThat(race.attemptedTable().get()).isNotNull();
        assertThat(race.winningTable().get()).isNotNull();
        assertThat(race.winningTable().get().getStorage().getLocation())
                .isNotEqualTo(race.attemptedTable().get().getStorage().getLocation());
        assertThat(metadataExists(race, race.attemptedTable().get()))
                .isFalse();
    }

    @Test
    void testCreateTableRacePreservesMetadataWhenWinnerLookupFails()
    {
        CreateRace race = createRace(UnaryOperator.identity(), true);

        assertThatThrownBy(() -> race.metadata().createTable(SESSION, race.tableMetadata(), SaveMode.FAIL))
                .isInstanceOf(TableAlreadyExistsException.class)
                .satisfies(failure -> assertThat(failure.getSuppressed()).hasSize(1));

        assertThat(race.attemptedTable().get()).isNotNull();
        assertThat(metadataExists(race, race.attemptedTable().get()))
                .isTrue();
    }

    private CreateRace createRace(UnaryOperator<Table> winningTableFactory, boolean failWinnerLookup)
    {
        SchemaTableName tableName = new SchemaTableName("test_schema", "concurrent_create");
        AtomicReference<Table> attemptedTable = new AtomicReference<>();
        AtomicReference<Table> winningTable = new AtomicReference<>();
        HiveMetastore metastore = (HiveMetastore) Proxy.newProxyInstance(
                HiveMetastore.class.getClassLoader(),
                new Class<?>[] {HiveMetastore.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getDatabase" -> Optional.of(Database.builder()
                            .setDatabaseName(tableName.getSchemaName())
                            .setLocation(Optional.of("local:///test_schema"))
                            .setOwnerName(Optional.of("public"))
                            .setOwnerType(Optional.of(PrincipalType.ROLE))
                            .build());
                    case "getTable" -> {
                        if (failWinnerLookup && attemptedTable.get() != null) {
                            throw new IllegalStateException("metastore unavailable during ownership check");
                        }
                        yield Optional.ofNullable(winningTable.get());
                    }
                    case "createTable" -> {
                        Table table = (Table) arguments[0];
                        attemptedTable.set(table);
                        winningTable.set(winningTableFactory.apply(table));
                        throw new TableAlreadyExistsException(tableName);
                    }
                    default -> throw new AssertionError("Unexpected metastore call: " + method);
                });
        TrinoFileSystemFactory fileSystemFactory = new LocalFileSystemFactory(temporaryDirectory);
        TypeManager unusedTypeManager = (TypeManager) Proxy.newProxyInstance(
                TypeManager.class.getClassLoader(),
                new Class<?>[] {TypeManager.class},
                (proxy, method, arguments) -> {
                    throw new AssertionError("Unexpected type manager call: " + method);
                });
        HudiMetadata metadata = new HudiMetadata(
                metastore,
                fileSystemFactory,
                unusedTypeManager,
                newDirectExecutorService());
        ConnectorTableMetadata tableMetadata = new ConnectorTableMetadata(
                tableName,
                List.of(ColumnMetadata.builder()
                        .setName("id")
                        .setType(BIGINT)
                        .build()));
        return new CreateRace(metadata, fileSystemFactory, tableMetadata, attemptedTable, winningTable);
    }

    private static boolean metadataExists(CreateRace race, Table table)
    {
        return HudiUtil.hudiMetadataExists(
                race.fileSystemFactory().create(SESSION),
                Location.of(table.getStorage().getLocation()));
    }

    private record CreateRace(
            HudiMetadata metadata,
            TrinoFileSystemFactory fileSystemFactory,
            ConnectorTableMetadata tableMetadata,
            AtomicReference<Table> attemptedTable,
            AtomicReference<Table> winningTable) {}
}
