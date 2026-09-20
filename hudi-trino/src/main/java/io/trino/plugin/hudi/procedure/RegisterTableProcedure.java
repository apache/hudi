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
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoFileSystem;
import io.trino.filesystem.TrinoFileSystemFactory;
import io.trino.metastore.HiveMetastore;
import io.trino.metastore.HiveMetastoreFactory;
import io.trino.plugin.hudi.HudiMetastoreTables;
import io.trino.plugin.hudi.HudiUtil;
import io.trino.spi.TrinoException;
import io.trino.spi.classloader.ThreadContextClassLoader;
import io.trino.spi.connector.ConnectorAccessControl;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.connector.SchemaNotFoundException;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.procedure.Procedure;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;

import java.lang.invoke.MethodHandle;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static io.trino.metastore.PrincipalPrivileges.NO_PRIVILEGES;
import static io.trino.plugin.base.util.Procedures.checkProcedureArgument;
import static io.trino.plugin.hudi.HudiTableProperties.LOCATION_PROPERTY;
import static io.trino.spi.StandardErrorCode.ALREADY_EXISTS;
import static io.trino.spi.type.VarcharType.VARCHAR;
import static java.lang.invoke.MethodHandles.lookup;
import static java.util.Objects.requireNonNull;

public class RegisterTableProcedure
        implements Provider<Procedure>
{
    private static final MethodHandle REGISTER_TABLE;

    static {
        try {
            REGISTER_TABLE = lookup().unreflect(RegisterTableProcedure.class.getMethod(
                    "registerTable",
                    ConnectorSession.class,
                    ConnectorAccessControl.class,
                    String.class,
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
    public RegisterTableProcedure(HiveMetastoreFactory metastoreFactory, TrinoFileSystemFactory fileSystemFactory)
    {
        this.metastoreFactory = requireNonNull(metastoreFactory, "metastoreFactory is null");
        this.fileSystemFactory = requireNonNull(fileSystemFactory, "fileSystemFactory is null");
    }

    @Override
    public Procedure get()
    {
        return new Procedure(
                "system",
                "register_table",
                ImmutableList.of(
                        new Procedure.Argument("SCHEMA_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_NAME", VARCHAR),
                        new Procedure.Argument("TABLE_LOCATION", VARCHAR)),
                REGISTER_TABLE.bindTo(this));
    }

    public void registerTable(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schemaName,
            String tableName,
            String tableLocation)
    {
        try (ThreadContextClassLoader _ = new ThreadContextClassLoader(getClass().getClassLoader())) {
            doRegisterTable(session, accessControl, schemaName, tableName, tableLocation);
        }
    }

    private void doRegisterTable(
            ConnectorSession session,
            ConnectorAccessControl accessControl,
            String schemaName,
            String tableName,
            String tableLocation)
    {
        checkProcedureArgument(schemaName != null, "schema_name cannot be null");
        checkProcedureArgument(tableName != null, "table_name cannot be null");
        checkProcedureArgument(tableLocation != null, "table_location cannot be null");

        SchemaTableName schemaTableName = new SchemaTableName(schemaName, tableName);
        String basePath = Location.of(tableLocation).toString();
        HiveMetastore metastore = metastoreFactory.createMetastore(Optional.of(session.getIdentity()));
        if (metastore.getDatabase(schemaName).isEmpty()) {
            throw new SchemaNotFoundException(schemaName);
        }
        if (metastore.getTable(schemaName, tableName).isPresent()) {
            throw new TrinoException(ALREADY_EXISTS, "Table already exists: " + schemaTableName);
        }

        accessControl.checkCanCreateTable(null, schemaTableName, Map.of(LOCATION_PROPERTY, basePath));

        TrinoFileSystem fileSystem = fileSystemFactory.create(session);
        HoodieTableMetaClient metaClient = HudiUtil.buildTableMetaClient(fileSystem, schemaTableName.toString(), basePath);
        HoodieSchema tableSchema = HudiUtil.getLatestTableSchema(metaClient, tableName);
        List<String> partitionFields = metaClient.getTableConfig().getPartitionFields()
                .map(Arrays::asList)
                .orElse(ImmutableList.of());

        metastore.createTable(
                HudiMetastoreTables.buildTable(
                        schemaTableName,
                        basePath,
                        metaClient.getTableType(),
                        tableSchema,
                        partitionFields,
                        true,
                        Optional.of(session.getUser()),
                        Optional.empty()),
                NO_PRIVILEGES);
    }
}
