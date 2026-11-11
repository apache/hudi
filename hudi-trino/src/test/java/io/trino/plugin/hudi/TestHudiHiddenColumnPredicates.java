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
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.QueryRunner;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiHiddenColumnPredicates
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
    public void testHiddenColumnPredicatesWithCaseResolution()
    {
        Session session = session(true, true);
        assertThat(computeActual(session, "SELECT count(*) FROM hudi_non_part_cow").getOnlyValue()).isEqualTo(2L);
        assertThat(computeActual(session, "SELECT count(*) FROM hudi_non_part_cow WHERE \"$path\" = 'not-a-file'").getOnlyValue()).isEqualTo(0L);
        assertThat(computeActual(session, "SELECT count(*) FROM hudi_non_part_cow WHERE id = 1 AND \"$path\" = 'not-a-file'").getOnlyValue()).isEqualTo(0L);
        assertThat(computeActual(session, "SELECT count(*) FROM hudi_non_part_cow WHERE \"$file_size\" > 0").getOnlyValue()).isEqualTo(2L);
        assertThat(computeActual(session, "SELECT count(*) FROM hudi_non_part_cow WHERE \"$file_modified_time\" IS NOT NULL").getOnlyValue()).isEqualTo(2L);
    }

    private Session session(boolean metadata, boolean casing)
    {
        return Session.builder(getSession())
                .setCatalogSessionProperty("hudi", "metadata_enabled", Boolean.toString(metadata))
                .setCatalogSessionProperty("hudi", "resolve_column_name_casing_enabled", Boolean.toString(casing))
                .build();
    }
}
