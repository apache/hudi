package org.apache.hudi.utilities.functional;

import org.apache.avro.Schema;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.testutils.FunctionalTestHarness;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.schema.HiveSchemaProvider;
import org.apache.hudi.utilities.testutils.UtilitiesTestBase;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

@Tag("functional")
public class TestHiveSchemaProvider extends FunctionalTestHarness {
    private static final Logger LOG = LogManager.getLogger(TestJdbcbasedSchemaProvider.class);
    private static final TypedProperties PROPS = new TypedProperties();
    private static final String SOURCE_SCHEMA_TABLE_NAME = "schema_registry.source_schema_tab";
    private static final String TARGET_SCHEMA_TABLE_NAME = "schema_registry.target_schema_tab";
    @BeforeAll
    public static void init() {
        Pair<String, String> dbAndTableName = getDBandTableName(SOURCE_SCHEMA_TABLE_NAME);
        PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.hive.database", dbAndTableName.getLeft());
        PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.hive.table", dbAndTableName.getRight());
    }

    @Test
    public void testSourceSchema() throws Exception {
        try {
            createSchemaTable(SOURCE_SCHEMA_TABLE_NAME);
            Schema sourceSchema = UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), PROPS, jsc()).getSourceSchema();
            assertEquals(
                    sourceSchema.toString().toUpperCase(),
                    new Schema.Parser().parse(
                            UtilitiesTestBase.Helpers.readFile("delta-streamer-config/hive_schema_provider_source.avsc")
                    ).toString().toUpperCase());
        } catch (HoodieException e) {
            LOG.error("Failed to get source schema. ", e);
        }
    }


    @Test
    public void testTargetSchema() throws Exception {
        try {
            Pair<String, String> dbAndTableName = getDBandTableName(TARGET_SCHEMA_TABLE_NAME);
            PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.hive.database", dbAndTableName.getLeft());
            PROPS.setProperty("hoodie.deltastreamer.schemaprovider.target.schema.hive.table", dbAndTableName.getRight());
            createSchemaTable(SOURCE_SCHEMA_TABLE_NAME);
            createSchemaTable(TARGET_SCHEMA_TABLE_NAME);
            Schema targetSchema = UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), PROPS, jsc()).getTargetSchema();
            assertEquals(
                    targetSchema.toString().toUpperCase(),
                    new Schema.Parser().parse(
                            UtilitiesTestBase.Helpers.readFile("delta-streamer-config/hive_schema_provider_target.avsc")
                    ).toString().toUpperCase());
        } catch (HoodieException e) {
            LOG.error("Failed to get source/target schema. ", e);
        }
    }

    @Test
    public void testNotExistTable() {
        String wrongName = "wrong_schema_tab";
        PROPS.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.hive.table", wrongName);
        Assertions.assertThrows(NoSuchTableException.class, () -> {
            try {
                UtilHelpers.createSchemaProvider(HiveSchemaProvider.class.getName(), PROPS, jsc()).getSourceSchema();
            } catch (Throwable exception) {
                while (exception.getCause() != null) {
                    exception = exception.getCause();
                }
                throw exception;
            }
        });
    }

    private static Pair<String, String> getDBandTableName(String fullName) {
        String[] dbAndTableName = fullName.split("\\.");
        if (dbAndTableName.length > 1) {
            return new ImmutablePair<>(dbAndTableName[0], dbAndTableName[1]);
        } else {
            return new ImmutablePair<>("default", dbAndTableName[0]);
        }
    }
    
    private void createSchemaTable(String fullName) throws IOException {
        String createTableSQL = UtilitiesTestBase.Helpers.readFile(String.format("delta-streamer-config/%s.sql", fullName));
        SparkSession spark = spark();
        Pair<String, String> dbAndTableName = getDBandTableName(fullName);
        spark.sql(String.format("CREATE DATABASE IF NOT EXISTS %s", dbAndTableName.getLeft()));
        spark.sql(createTableSQL);
        spark.sql(String.format("SHOW CREATE TABLE %s.%s", dbAndTableName.getLeft(), dbAndTableName.getRight())).show(false);
    }
}
