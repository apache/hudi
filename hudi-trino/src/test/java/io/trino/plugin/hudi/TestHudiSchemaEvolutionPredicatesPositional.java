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

import io.trino.plugin.hudi.testing.SchemaEvolutionHudiTablesInitializer;
import io.trino.testing.QueryRunner;

/**
 * {@link TestHudiSchemaEvolutionPredicates} with {@code hudi.parquet.use-column-names=false}, so columns are resolved
 * by ordinal instead of by name. apache/hudi#19457 was reported against both modes and neither has anything to do
 * with the other: resolution decides WHICH file column a handle denotes, and the failure is about what its
 * statistics are made of once it has been found. Running the suite twice is what keeps a fix aimed at one resolution
 * path from quietly leaving the other broken.
 */
public class TestHudiSchemaEvolutionPredicatesPositional
        extends TestHudiSchemaEvolutionPredicates
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addConnectorProperty("hudi.parquet.use-column-names", "false")
                .setDataLoader(new SchemaEvolutionHudiTablesInitializer())
                .build();
    }
}
