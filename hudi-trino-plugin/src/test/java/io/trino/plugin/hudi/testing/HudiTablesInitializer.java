


package io.trino.plugin.hudi.testing;

import io.trino.filesystem.Location;
import io.trino.testing.QueryRunner;

public interface HudiTablesInitializer
{
    void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception;
}
