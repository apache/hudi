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

import io.trino.testing.BaseConnectorSmokeTest;
import io.trino.testing.TestingConnectorBehavior;
import io.trino.testing.sql.TestTable;
import org.junit.jupiter.api.Test;

import static io.trino.spi.connector.ConnectorMetadata.MODIFYING_ROWS_MESSAGE;
import static org.assertj.core.api.Assertions.assertThat;

public abstract class BaseHudiConnectorSmokeTest
        extends BaseConnectorSmokeTest
{
    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_INSERT,
                 SUPPORTS_DELETE,
                 SUPPORTS_UPDATE,
                 SUPPORTS_MERGE,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_COMMENT_ON_COLUMN -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        // Override because Hudi connector contains 'location' table property
        String schema = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE region"))
                .matches("\\QCREATE TABLE hudi." + schema + ".region (\n" +
                        "   regionkey bigint,\n" +
                        "   name varchar(25),\n" +
                        "   comment varchar(152)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/region'\n\\Q" +
                        ")");
    }

    @Test
    @Override
    public void verifySupportsDeleteDeclaration()
    {
        assertModificationUnsupported("DELETE FROM %s");
    }

    @Test
    @Override
    public void verifySupportsRowLevelDeleteDeclaration()
    {
        assertModificationUnsupported("DELETE FROM %s WHERE a = 1");
    }

    @Test
    @Override
    public void verifySupportsUpdateDeclaration()
    {
        assertModificationUnsupported("UPDATE %s SET a = 1");
    }

    @Test
    @Override
    public void verifySupportsRowLevelUpdateDeclaration()
    {
        assertModificationUnsupported("UPDATE %s SET a = a + 1 WHERE b = 2");
    }

    private void assertModificationUnsupported(String statement)
    {
        // The inherited checks use CTAS for setup, but Hudi supports only empty CREATE TABLE in
        // this stage. An empty table is sufficient to verify the DELETE/UPDATE capability contract.
        try (TestTable table = newTrinoTable("test_unsupported_modification", getCreateTableDefaultDefinition())) {
            assertQueryFails(statement.formatted(table.getName()), MODIFYING_ROWS_MESSAGE);
        }
    }
}
