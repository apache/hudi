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
package io.trino.plugin.hudi.query.index;

import io.airlift.log.Logger;
import io.trino.spi.connector.SchemaTableName;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieIndexDefinition;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.TableSchemaResolver;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.metadata.HoodieTableMetadataUtil;
import org.apache.hudi.util.Lazy;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public abstract class HudiBaseIndexSupport
        implements HudiIndexSupport
{
    private final Logger log;
    protected final SchemaTableName schemaTableName;
    protected final Lazy<HoodieTableMetaClient> lazyMetaClient;

    public HudiBaseIndexSupport(Logger log, SchemaTableName schemaTableName, Lazy<HoodieTableMetaClient> lazyMetaClient)
    {
        this.log = requireNonNull(log, "log is null");
        this.schemaTableName = requireNonNull(schemaTableName, "schemaTableName is null");
        this.lazyMetaClient = requireNonNull(lazyMetaClient, "metaClient is null");
    }

    public void printDebugMessage(Map<String, List<FileSlice>> candidateFileSlices, Map<String, List<FileSlice>> inputFileSlices, long lookupDurationMs)
    {
        if (log.isDebugEnabled()) {
            int candidateFileSize = candidateFileSlices.values().stream().mapToInt(List::size).sum();
            int totalFiles = inputFileSlices.values().stream().mapToInt(List::size).sum();
            double skippingPercent = totalFiles == 0 ? 0.0d : (totalFiles - candidateFileSize) / (totalFiles * 1.0d);

            log.info("Total files: %s; files after data skipping: %s; skipping percent %s; time taken: %s ms; table name: %s",
                    totalFiles,
                    candidateFileSize,
                    skippingPercent,
                    lookupDurationMs,
                    schemaTableName);
        }
    }

    protected Map<String, HoodieIndexDefinition> getAllIndexDefinitions()
    {
        if (lazyMetaClient.get().getIndexMetadata().isEmpty()) {
            return Map.of();
        }

        return lazyMetaClient.get().getIndexMetadata().get().getIndexDefinitions();
    }

    /**
     * Resolves the columns covered by a stats index partition (column stats or partition stats).
     *
     * <p>Prefers the registered index definition, but tables written by this release line do not
     * register one for the built-in stats partitions -- only secondary, expression and record
     * indexes get a definition here. So fall back to deriving the columns the same way the writer
     * chooses them, which is what the Spark reader on this branch does as well: it gates only on
     * the metadata partition being present and resolves the indexed columns separately.
     *
     * <p>A column wrongly treated as indexed is safe: no stats come back for it, and both
     * {@code shouldSkipFileSlice} and {@code evaluateStatisticPredicate} keep the file when stats
     * are missing. The cost of being wrong is a missed pruning opportunity, never a wrong result.
     *
     * @return the indexed columns, or an empty list if they cannot be determined (index disabled)
     */
    protected List<String> resolveStatsIndexedColumns(String indexPartitionPath)
    {
        HoodieIndexDefinition definition = getAllIndexDefinitions().get(indexPartitionPath);
        if (definition != null && definition.getSourceFields() != null && !definition.getSourceFields().isEmpty()) {
            return definition.getSourceFields();
        }

        HoodieTableMetaClient metaClient = lazyMetaClient.get();
        try {
            Option<HoodieSchema> tableSchema = Option.of(new TableSchemaResolver(metaClient).getTableSchema());
            return List.copyOf(HoodieTableMetadataUtil.getColumnsToIndex(
                            metaClient.getTableConfig(),
                            HoodieMetadataConfig.newBuilder().enable(true).build(),
                            Lazy.eagerly(tableSchema),
                            Option.empty(),
                            HoodieTableMetadataUtil.existingIndexVersionOrDefault(indexPartitionPath, metaClient))
                    .keySet());
        }
        catch (Exception e) {
            log.warn(e, "Could not derive the indexed columns of %s for table %s, skipping the index", indexPartitionPath, schemaTableName);
            return List.of();
        }
    }
}
