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

import com.google.common.collect.ImmutableSet;
import io.opentelemetry.sdk.trace.data.SpanData;
import io.trino.metadata.Metadata;
import io.trino.metadata.QualifiedObjectName;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer;
import io.trino.plugin.hudi.testing.ResourceHudiTablesInitializer.TestingTable;
import io.trino.testing.AbstractTestQueryFramework;
import io.trino.testing.DistributedQueryRunner;
import io.trino.testing.QueryRunner;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.filesystem.tracing.FileSystemAttributes.FILE_LOCATION;
import static io.trino.testing.TransactionBuilder.transaction;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.parallel.ExecutionMode.SAME_THREAD;

/**
 * Asserts that reading file groups with log files on a worker does not touch the table metadata under
 * {@code .hoodie}: the table config, schema and timeline come from the table handle built on the coordinator.
 */
@Execution(SAME_THREAD)
final class TestHudiWorkerTableMetadataAccess
        extends AbstractTestQueryFramework
{
    // Spans the task executors open around split processing; file system spans under them come from the page source path
    private static final Set<String> SPLIT_PROCESSING_SPANS = ImmutableSet.of("process", "split");

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return HudiQueryRunner.builder()
                .addConnectorProperty("fs.cache.enabled", "false")
                .addConnectorProperty("hudi.table-statistics-enabled", "false")
                .setDataLoader(new ResourceHudiTablesInitializer())
                .setWorkerCount(1)
                .build();
    }

    @ParameterizedTest
    @EnumSource(value = TestingTable.class, names = {"HUDI_COMPREHENSIVE_TYPES_V6_MOR", "HUDI_COMPREHENSIVE_TYPES_V8_MOR"})
    void testLogFileSplitsDoNotReadTableMetadata(TestingTable table)
    {
        DistributedQueryRunner queryRunner = getDistributedQueryRunner();
        queryRunner.executeWithPlan(getSession(), "SELECT * FROM " + table.getRtTableName());

        String tableDirectory = "/" + table.getTableName() + "/";
        List<String> splitFileLocations = splitProcessingFileLocations(queryRunner.getSpans()).stream()
                .filter(location -> location.contains(tableDirectory))
                .collect(toImmutableList());
        assertThat(splitFileLocations).anyMatch(location -> location.contains(".log."));
        assertThat(splitFileLocations).noneMatch(location -> location.contains("/.hoodie"));
    }

    @ParameterizedTest
    @EnumSource(value = TestingTable.class, names = {"HUDI_COMPREHENSIVE_TYPES_V6_MOR", "HUDI_COMPREHENSIVE_TYPES_V8_MOR"})
    void testTableHandleCarriesWorkerTableState(TestingTable table)
    {
        HudiTableHandle handle = coordinatorTableHandle(table.getRtTableName());

        assertThat(handle.getTableSchemaStr()).isNotEmpty();
        assertThat(handle.getTableConfig())
                .containsKey(HoodieTableConfig.VERSION.key())
                .doesNotContainKey(HoodieTableConfig.CREATE_SCHEMA.key());
        assertThat(handle.getCommittedInstants().isPresent())
                .isEqualTo(handle.getFileGroupReaderTableState().getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT));
    }

    private HudiTableHandle coordinatorTableHandle(String tableName)
    {
        QueryRunner queryRunner = getQueryRunner();
        Metadata metadata = queryRunner.getPlannerContext().getMetadata();
        return transaction(queryRunner.getTransactionManager(), metadata, queryRunner.getAccessControl())
                .readOnly()
                .execute(getSession(), session -> {
                    QualifiedObjectName name = new QualifiedObjectName(session.getCatalog().orElseThrow(), session.getSchema().orElseThrow(), tableName);
                    return (HudiTableHandle) metadata.getTableHandle(session, name).orElseThrow().connectorHandle();
                });
    }

    private static List<String> splitProcessingFileLocations(List<SpanData> spans)
    {
        Map<String, SpanData> spansById = spans.stream()
                .collect(toImmutableMap(SpanData::getSpanId, Function.identity(), (first, _) -> first));
        return spans.stream()
                .filter(span -> span.getAttributes().get(FILE_LOCATION) != null)
                .filter(span -> hasSplitProcessingAncestor(span, spansById))
                .map(span -> span.getAttributes().get(FILE_LOCATION))
                .collect(toImmutableList());
    }

    private static boolean hasSplitProcessingAncestor(SpanData span, Map<String, SpanData> spansById)
    {
        SpanData parent = spansById.get(span.getParentSpanId());
        while (parent != null) {
            if (SPLIT_PROCESSING_SPANS.contains(parent.getName())) {
                return true;
            }
            parent = spansById.get(parent.getParentSpanId());
        }
        return false;
    }
}
