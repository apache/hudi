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

import io.trino.plugin.hudi.testing.TpchHudiTablesInitializer;
import io.trino.testing.BaseConnectorTest;
import io.trino.testing.QueryRunner;
import io.trino.testing.TestingConnectorBehavior;
import org.junit.jupiter.api.Test;

import java.util.OptionalInt;

import static io.trino.plugin.hudi.testing.HudiTestUtils.COLUMNS_TO_HIDE;
import static io.trino.testing.TestingNames.randomNameSuffix;
import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiConnectorTest
        extends BaseConnectorTest
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addConnectorProperty("hudi.columns-to-hide", COLUMNS_TO_HIDE)
                .setDataLoader(new TpchHudiTablesInitializer(REQUIRED_TPCH_TABLES))
                .build();
    }

    @Override
    protected boolean hasBehavior(TestingConnectorBehavior connectorBehavior)
    {
        return switch (connectorBehavior) {
            case SUPPORTS_ADD_COLUMN,
                 SUPPORTS_COMMENT_ON_COLUMN,
                 SUPPORTS_COMMENT_ON_TABLE,
                 SUPPORTS_CREATE_MATERIALIZED_VIEW,
                 SUPPORTS_CREATE_SCHEMA,
                 SUPPORTS_CREATE_TABLE_WITH_DATA,
                 SUPPORTS_CREATE_VIEW,
                 SUPPORTS_DEFAULT_COLUMN_VALUE,
                 SUPPORTS_DELETE,
                 SUPPORTS_DEREFERENCE_PUSHDOWN,
                 SUPPORTS_INSERT,
                 // HudiMetadata.applyLimit returns limitGuaranteed=false (multi-split connector
                 // cannot bound total rows across workers). BaseConnectorTest.testLimitPushdown
                 // requires Output->TableScan with no Limit node, which needs guaranteed=true.
                 // Stays off, matching Iceberg / Delta Lake / Hive.
                 SUPPORTS_LIMIT_PUSHDOWN,
                 SUPPORTS_MERGE,
                 SUPPORTS_NOT_NULL_CONSTRAINT,
                 SUPPORTS_RENAME_COLUMN,
                 SUPPORTS_RENAME_TABLE,
                 SUPPORTS_SET_COLUMN_TYPE,
                 SUPPORTS_TOPN_PUSHDOWN,
                 SUPPORTS_UPDATE -> false;
            default -> super.hasBehavior(connectorBehavior);
        };
    }

    @Override
    protected OptionalInt maxTableNameLength()
    {
        // The test connector uses FileHiveMetastore, which applies the Hive-compatible limit.
        return OptionalInt.of(128);
    }

    @Override
    protected void verifyTableNameLengthFailurePermissible(Throwable failure)
    {
        assertThat(failure).hasMessageContaining("Table name must be shorter than or equal to '128' characters");
    }

    @Test
    @Override
    public void testCharVarcharComparison()
    {
        skipTestUnless(hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        super.testCharVarcharComparison();
    }

    @Test
    @Override
    public void testVarcharCharComparison()
    {
        skipTestUnless(hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        super.testVarcharCharComparison();
    }

    @Test
    @Override
    public void testCharToVarcharCastCoercionAcrossPushdown()
    {
        skipTestUnless(hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        super.testCharToVarcharCastCoercionAcrossPushdown();
    }

    @Test
    @Override
    public void testCreateTableAsSelectWithUnicode()
    {
        skipTestUnless(hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        super.testCreateTableAsSelectWithUnicode();
    }

    @Test
    @Override
    public void testColumnName()
    {
        // The inherited test creates an empty table and then inserts rows into it.
        skipTestUnless(hasBehavior(TestingConnectorBehavior.SUPPORTS_INSERT));
        super.testColumnName();
    }

    @Test
    @Override
    public void testDataMappingSmokeTest()
    {
        skipTestUnless(hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        super.testDataMappingSmokeTest();
    }

    @Test
    @Override
    public void testCaseSensitiveDataMapping()
    {
        skipTestUnless(hasBehavior(TestingConnectorBehavior.SUPPORTS_CREATE_TABLE_WITH_DATA));
        super.testCaseSensitiveDataMapping();
    }

    @Test
    @Override
    public void testRenameTable()
    {
        // The inherited negative test uses CTAS for setup even when table rename is unsupported.
        String tableName = "test_rename_" + randomNameSuffix();
        String renamedTableName = "test_rename_new_" + randomNameSuffix();
        assertUpdate("CREATE TABLE " + tableName + " (x integer)");
        try {
            assertQueryFails(
                    "ALTER TABLE " + tableName + " RENAME TO " + renamedTableName,
                    "This connector does not support renaming tables");
        }
        finally {
            assertUpdate("DROP TABLE " + tableName);
        }
    }

    @Test
    @Override
    public void testShowCreateTable()
    {
        String schema = getSession().getSchema().orElseThrow();
        assertThat((String) computeScalar("SHOW CREATE TABLE orders"))
                .matches("\\QCREATE TABLE hudi." + schema + ".orders (\n" +
                        "   orderkey bigint,\n" +
                        "   custkey bigint,\n" +
                        "   orderstatus varchar(1),\n" +
                        "   totalprice double,\n" +
                        "   orderdate date,\n" +
                        "   orderpriority varchar(15),\n" +
                        "   clerk varchar(15),\n" +
                        "   shippriority integer,\n" +
                        "   comment varchar(79)\n" +
                        ")\n" +
                        "WITH (\n" +
                        "   location = \\E'.*/orders'\n\\Q" +
                        ")");
    }

    @Test
    public void testHideHiveSysSchema()
    {
        assertThat(computeActual("SHOW SCHEMAS").getOnlyColumnAsSet()).doesNotContain("sys");
        assertQueryFails("SHOW TABLES IN hudi.sys", ".*Schema 'sys' does not exist");
    }
}
