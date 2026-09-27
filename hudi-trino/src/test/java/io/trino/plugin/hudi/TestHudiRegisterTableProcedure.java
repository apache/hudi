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

import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.Database;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hudi.procedure.RegisterTableProcedure;
import io.trino.plugin.hudi.procedure.UnregisterTableProcedure;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSecurityContext;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.security.AccessDeniedException;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.testing.TestingConnectorSession.SESSION;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHudiRegisterTableProcedure
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .setDataLoader((queryRunner, externalLocation, schemaName) -> {})
                .build();
    }

    @Test
    void testDisabledByDefaultBeforeValidatingArguments()
    {
        assertQueryFails(
                "CALL hudi.system.register_table(NULL, NULL, NULL)",
                ".*register_table procedure is disabled.*");
    }

    @Test
    void testRegisterChecksAccessBeforeMetastoreLookup()
    {
        List<SchemaTableName> checkedTables = new ArrayList<>();
        ConnectorAccessControl accessControl = new ConnectorAccessControl()
        {
            @Override
            public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties)
            {
                checkedTables.add(tableName);
                throw new AccessDeniedException("denied");
            }
        };

        assertThatThrownBy(() -> registerProcedure(failOnMetastoreAccess())
                .registerTable(SESSION, accessControl, "Mixed_Schema", "Mixed_Table", "memory:///table"))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(checkedTables).singleElement().satisfies(tableName -> {
            assertThat(tableName.getSchemaName()).isEqualTo("mixed_schema");
            assertThat(tableName.getTableName()).isEqualTo("mixed_table");
        });
    }

    @Test
    void testUnregisterChecksAccessBeforeMetastoreLookup()
    {
        List<SchemaTableName> checkedTables = new ArrayList<>();
        ConnectorAccessControl accessControl = new ConnectorAccessControl()
        {
            @Override
            public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName)
            {
                checkedTables.add(tableName);
                throw new AccessDeniedException("denied");
            }
        };

        assertThatThrownBy(() -> unregisterProcedure(failOnMetastoreAccess())
                .unregisterTable(SESSION, accessControl, "Mixed_Schema", "Mixed_Table"))
                .isInstanceOf(AccessDeniedException.class);
        assertThat(checkedTables).singleElement().satisfies(tableName -> {
            assertThat(tableName.getSchemaName()).isEqualTo("mixed_schema");
            assertThat(tableName.getTableName()).isEqualTo("mixed_table");
        });
    }

    @Test
    void testRegisterUsesNormalizedNamesForMetastoreLookups()
    {
        List<String> lookups = new ArrayList<>();
        HiveMetastore metastore = metastoreProxy((proxy, method, arguments) -> {
            if (method.getName().equals("getDatabase")) {
                lookups.add("database:" + arguments[0]);
                return Optional.of(new Database(
                        "mixed_schema", Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty(), Map.of()));
            }
            if (method.getName().equals("getTable")) {
                lookups.add("table:" + arguments[0] + "." + arguments[1]);
                throw new LookupReached();
            }
            throw new AssertionError("Unexpected metastore call: " + method.getName());
        });

        assertThatThrownBy(() -> registerProcedure(metastore)
                .registerTable(SESSION, allowTableDdl(), "Mixed_Schema", "Mixed_Table", "memory:///table"))
                .isInstanceOf(LookupReached.class);
        assertThat(lookups).containsExactly("database:mixed_schema", "table:mixed_schema.mixed_table");
    }

    @Test
    void testUnregisterUsesNormalizedNamesForMetastoreLookup()
    {
        List<String> lookups = new ArrayList<>();
        HiveMetastore metastore = metastoreProxy((proxy, method, arguments) -> {
            if (method.getName().equals("getTable")) {
                lookups.add(arguments[0] + "." + arguments[1]);
                throw new LookupReached();
            }
            throw new AssertionError("Unexpected metastore call: " + method.getName());
        });

        assertThatThrownBy(() -> unregisterProcedure(metastore)
                .unregisterTable(SESSION, allowTableDdl(), "Mixed_Schema", "Mixed_Table"))
                .isInstanceOf(LookupReached.class);
        assertThat(lookups).containsExactly("mixed_schema.mixed_table");
    }

    private static RegisterTableProcedure registerProcedure(HiveMetastore metastore)
    {
        return new RegisterTableProcedure(
                HiveMetastoreFactory.ofInstance(metastore, false),
                failOnFileSystemAccess(),
                new HudiConfig().setRegisterTableProcedureEnabled(true));
    }

    private static UnregisterTableProcedure unregisterProcedure(HiveMetastore metastore)
    {
        return new UnregisterTableProcedure(HiveMetastoreFactory.ofInstance(metastore, false), failOnFileSystemAccess());
    }

    private static TrinoFileSystemFactory failOnFileSystemAccess()
    {
        return identity -> {
            throw new AssertionError("File system should not be accessed");
        };
    }

    private static ConnectorAccessControl allowTableDdl()
    {
        return new ConnectorAccessControl()
        {
            @Override
            public void checkCanCreateTable(ConnectorSecurityContext context, SchemaTableName tableName, Map<String, Object> properties) {}

            @Override
            public void checkCanDropTable(ConnectorSecurityContext context, SchemaTableName tableName) {}
        };
    }

    private static HiveMetastore failOnMetastoreAccess()
    {
        return metastoreProxy((proxy, method, arguments) -> {
            throw new AssertionError("Metastore should not be accessed before authorization");
        });
    }

    private static HiveMetastore metastoreProxy(InvocationHandler handler)
    {
        return (HiveMetastore) Proxy.newProxyInstance(
                TestHudiRegisterTableProcedure.class.getClassLoader(),
                new Class<?>[] {HiveMetastore.class},
                handler);
    }

    private static final class LookupReached
            extends RuntimeException
    {
    }
}
