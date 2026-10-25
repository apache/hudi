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
package io.trino.plugin.hudi.testing;

import com.google.common.collect.ImmutableList;
import io.trino.filesystem.Location;
import io.trino.testing.QueryRunner;

import java.util.List;

/**
 * Runs several {@link HudiTablesInitializer}s against one query runner, in order, so a single test class can
 * load more than one table fixture. Each delegate must create its own tables under a distinct name, and must
 * finish its writes within {@code initializeTables}: a fixture that keeps its write client open (see
 * {@code AbstractMergerHudiTablesInitializer.keepsWriterOpen}) cannot be composed here, because
 * {@link HudiTablesInitializer} declares no close for this class to forward.
 */
public class CompositeHudiTablesInitializer
        implements HudiTablesInitializer
{
    private final List<HudiTablesInitializer> delegates;

    public CompositeHudiTablesInitializer(HudiTablesInitializer... delegates)
    {
        this.delegates = ImmutableList.copyOf(delegates);
    }

    @Override
    public void initializeTables(QueryRunner queryRunner, Location externalLocation, String schemaName)
            throws Exception
    {
        for (HudiTablesInitializer delegate : delegates) {
            delegate.initializeTables(queryRunner, externalLocation, schemaName);
        }
    }
}
