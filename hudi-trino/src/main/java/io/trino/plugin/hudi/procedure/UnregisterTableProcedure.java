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
package io.trino.plugin.hudi.procedure;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.metastore.Table;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.connector.TableNotFoundException;
import io.trino.spi.procedure.Procedure;

import java.lang.invoke.MethodHandle;
import java.util.Optional;

import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hive.util.HiveUtil.isHudiTable;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class UnregisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle UNREGISTER_TABLE;

    static {
        try {
            UNREGISTER_TABLE = lookup().unreflect(UnregisterTableProcedure.class.getMethod(
                    "unregisterTable",
                    ConnectorSession.class,
                    ConnectorAccessControl.class,
                    String.class,
                    String.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private final HiveMetastoreFactory metastoreFactory;
    private final TrinoFileSystemFactory fileSystemFactory;

    @Inject
    public UnregisterTableProcedure(HiveMetastoreFactory metastoreFactory, TrinoFileSystemFactory fileSystemFactory)
    {
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "unregister_table",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR)),
                UNREGISTER_TABLE.bindTo(this));
    }

    public void unregisterTable(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schemaName,
            String tableName)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doUnregisterTable(session, accessControl, schemaName, tableName);
        }
    }

    private void doUnregisterTable(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schemaName,
            String tableName)
    {
        checkProcedureArgument(schemaName != null, "schema_name cannot be null");
        checkProcedureArgument(tableName != null, "table_name cannot be null");

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(session.getIdentity()));
        Table table = metastore.getTable(schemaName, tableName)
                .orElseThrow(() -> new TableNotFoundException(schemaTableName));

        accessControl.checkCanDropTable(null, schemaTableName);

        // A malformed input-format entry is precisely one reason this recovery procedure exists.
        // When the metastore no longer identifies the entry as Hudi, verify its storage instead of
        // making it impossible to detach through this connector.
        if (!isHudiTable(table)) {
            HudiUtil.buildTableMetaClient(
                    fileSystemFactory.create(session),
                    schemaTableName.toString(),
                    table.getStorage().getLocation());
        }

        // Never delete data. This is the semantic difference from DROP TABLE for a managed table.
        metastore.dropTable(schemaName, tableName, false);
    }
}
