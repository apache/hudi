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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.log.Logger;
import io.trino.metastore.Table;
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorTableHandle;
import io.trino.spi.connector.SchemaTableName;
import io.trino.spi.predicate.TupleDomain;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.FileGroupReaderTableState;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.Lazy;
import org.apache.hudi.storage.StoragePath;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.function.Supplier;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static io.trino.spi.connector.SchemaTableName.schemaTableName;
import static java.util.Objects.requireNonNull;
import static java.util.Objects.requireNonNullElse;
import static java.util.function.Function.identity;

public class HudiTableHandle
        implements ConnectorTableHandle
{
    private static final Logger log = Logger.get(HudiTableHandle.class);
    private final String schemaName;
    private final String tableName;
    private final String basePath;
    private final HoodieTableType tableType;
    private final List<HiveColumnHandle> partitionColumns;
    private final Lazy<List<HiveColumnHandle>> lazyMergeRequiredColumns;
    // Used only for validation when config property hudi.query-partition-filter-required is enabled
    private final Set<HiveColumnHandle> constraintColumns;
    private final TupleDomain<HiveColumnHandle> partitionPredicates;
    private final TupleDomain<HiveColumnHandle> regularPredicates;
    private final OptionalLong limit;
    private final Optional<Lazy<HoodieSchema>> hudiTableSchema;
    private final Lazy<Map<String, String>> lazyTableConfig;
    private final Lazy<Optional<HudiCommittedInstants>> lazyCommittedInstants;
    private final Lazy<FileGroupReaderTableState> lazyFileGroupReaderTableState;
    // Coordinator-only
    private final transient Optional<Table> table;
    private final transient Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient;
    private final transient Lazy<String> lazyLatestCommitTime;

    @JsonCreator
    public HudiTableHandle(
            @JsonProperty("schemaName") String schemaName,
            @JsonProperty("tableName") String tableName,
            @JsonProperty("basePath") String basePath,
            @JsonProperty("tableType") HoodieTableType tableType,
            @JsonProperty("partitionColumns") List<HiveColumnHandle> partitionColumns,
            @JsonProperty("mergeRequiredColumns") List<HiveColumnHandle> mergeRequiredColumns,
            @JsonProperty("partitionPredicates") TupleDomain<HiveColumnHandle> partitionPredicates,
            @JsonProperty("regularPredicates") TupleDomain<HiveColumnHandle> regularPredicates,
            @JsonProperty("limit") OptionalLong limit,
            @JsonProperty("tableSchemaStr") String tableSchemaStr,
            @JsonProperty("latestCommitTime") String latestCommitTime,
            @JsonProperty("tableConfig") Map<String, String> tableConfig,
            @JsonProperty("committedInstants") Optional<HudiCommittedInstants> committedInstants)
    {
        this(Optional.empty(), Optional.empty(), schemaName, tableName, basePath, tableType, partitionColumns, Lazy.eagerly(mergeRequiredColumns), ImmutableSet.of(),
                partitionPredicates, regularPredicates, limit, buildTableSchema(tableSchemaStr), () -> latestCommitTime,
                Lazy.eagerly(ImmutableMap.copyOf(requireNonNullElse(tableConfig, ImmutableMap.of()))),
                Lazy.eagerly(requireNonNullElse(committedInstants, Optional.empty())));
    }

    public HudiTableHandle(
            Table table,
            Lazy<HoodieTableMetaClient> lazyMetaClient,
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            List<HiveColumnHandle> partitionColumns,
            Lazy<List<HiveColumnHandle>> lazyMergeRequiredColumns,
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates,
            OptionalLong limit,
            Optional<Lazy<HoodieSchema>> hudiTableSchema)
    {
        this(
                table,
                lazyMetaClient,
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                lazyMergeRequiredColumns,
                constraintColumns,
                partitionPredicates,
                regularPredicates,
                limit,
                hudiTableSchema,
                Lazy.lazily(() -> lazyMetaClient
                        .get()
                        .getActiveTimeline()
                        .getCommitsTimeline()
                        .filterCompletedInstants()
                        .lastInstant()
                        .map(HoodieInstant::requestedTime)
                        .orElseThrow(() -> new TrinoException(
                                HudiErrorCode.HUDI_NO_VALID_COMMIT,
                                "Table has no valid commits"))));
    }

    private HudiTableHandle(
            Table table,
            Lazy<HoodieTableMetaClient> lazyMetaClient,
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            List<HiveColumnHandle> partitionColumns,
            Lazy<List<HiveColumnHandle>> lazyMergeRequiredColumns,
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates,
            OptionalLong limit,
            Optional<Lazy<HoodieSchema>> hudiTableSchema,
            Lazy<String> lazyLatestCommitTime)
    {
        this(
                Optional.of(table),
                Optional.of(lazyMetaClient),
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                lazyMergeRequiredColumns,
                constraintColumns,
                partitionPredicates,
                regularPredicates,
                limit,
                hudiTableSchema,
                lazyLatestCommitTime::get,
                Lazy.lazily(() -> toMap(lazyMetaClient.get().getTableConfig())),
                Lazy.lazily(() -> captureCommittedInstants(lazyMetaClient.get(), tableType, lazyLatestCommitTime.get())));
    }

    HudiTableHandle(
            Optional<Table> table,
            Optional<Lazy<HoodieTableMetaClient>> lazyMetaClient,
            String schemaName,
            String tableName,
            String basePath,
            HoodieTableType tableType,
            List<HiveColumnHandle> partitionColumns,
            Lazy<List<HiveColumnHandle>> lazyMergeRequiredColumns,
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionPredicates,
            TupleDomain<HiveColumnHandle> regularPredicates,
            OptionalLong limit,
            Optional<Lazy<HoodieSchema>> hudiTableSchema,
            Supplier<String> latestCommitTimeSupplier,
            Lazy<Map<String, String>> lazyTableConfig,
            Lazy<Optional<HudiCommittedInstants>> lazyCommittedInstants)
    {
        this.table = requireNonNull(table, "table is null");
        this.lazyMetaClient = requireNonNull(lazyMetaClient, "lazyMetaClient is null");
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");
        this.basePath = requireNonNull(basePath, "basePath is null");
        this.tableType = requireNonNull(tableType, "tableType is null");
        this.partitionColumns = requireNonNull(partitionColumns, "partitionColumns is null");
        this.lazyMergeRequiredColumns = requireNonNull(lazyMergeRequiredColumns, "lazyMergeRequiredColumns is null");
        this.constraintColumns = requireNonNull(constraintColumns, "constraintColumns is null");
        this.partitionPredicates = requireNonNull(partitionPredicates, "partitionPredicates is null");
        this.regularPredicates = requireNonNull(regularPredicates, "regularPredicates is null");
        this.limit = requireNonNull(limit, "limit is null");
        this.hudiTableSchema = requireNonNull(hudiTableSchema, "hudiTableSchema is null");
        this.lazyLatestCommitTime = Lazy.lazily(latestCommitTimeSupplier);
        this.lazyTableConfig = requireNonNull(lazyTableConfig, "lazyTableConfig is null");
        this.lazyCommittedInstants = requireNonNull(lazyCommittedInstants, "lazyCommittedInstants is null");
        this.lazyFileGroupReaderTableState = Lazy.lazily(() -> buildFileGroupReaderTableState(basePath, lazyTableConfig.get(), lazyCommittedInstants.get()));
    }

    /**
     * The table properties workers need. The create schema is left out: it can be as large as the table schema, and
     * workers read the schema from {@link #getTableSchemaStr()}.
     */
    private static Map<String, String> toMap(HoodieTableConfig tableConfig)
    {
        Properties properties = tableConfig.getProps();
        return properties.stringPropertyNames().stream()
                .filter(key -> !key.equals(HoodieTableConfig.CREATE_SCHEMA.key()))
                .collect(toImmutableMap(identity(), properties::getProperty));
    }

    /**
     * Only file groups with log files need the committed instants, and only for tables before version 8, whose log
     * blocks the reader checks against the timeline.
     */
    private static Optional<HudiCommittedInstants> captureCommittedInstants(HoodieTableMetaClient metaClient, HoodieTableType tableType, String latestCommitTime)
    {
        if (tableType != HoodieTableType.MERGE_ON_READ || !metaClient.getTableConfig().getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
            return Optional.empty();
        }
        return Optional.of(HudiCommittedInstants.capture(metaClient.getCommitsTimeline(), latestCommitTime));
    }

    private static FileGroupReaderTableState buildFileGroupReaderTableState(
            String basePath,
            Map<String, String> tableConfigMap,
            Optional<HudiCommittedInstants> committedInstants)
    {
        checkState(!tableConfigMap.isEmpty(), "Table handle for %s carries no table config", basePath);
        HoodieTableConfig tableConfig = new HoodieTableConfig();
        tableConfig.getProps().putAll(tableConfigMap);
        return FileGroupReaderTableState.of(
                new StoragePath(basePath),
                tableConfig,
                Option.fromJavaOptional(committedInstants.map(HudiCommittedInstants::toCommittedInstants)),
                Option.empty());
    }

    /**
     * Builds a lazily-parsed schema from the given Avro schema JSON string.
     * <p>
     * Returns {@code Optional.empty()} if the input string is null/empty
     * or if parsing the schema fails.
     */
    private static Optional<Lazy<HoodieSchema>> buildTableSchema(String tableSchemaStr)
    {
        if (StringUtils.isNullOrEmpty(tableSchemaStr)) {
            return Optional.empty();
        }

        try {
            Lazy<HoodieSchema> lazySchema = Lazy.lazily(() -> HoodieSchema.parse(tableSchemaStr));
            return Optional.of(lazySchema);
        }
        catch (Exception e) {
            log.warn(e, "Failed to parse table schema: %s", tableSchemaStr);
            return Optional.empty();
        }
    }

    public Table getTable()
    {
        checkArgument(table.isPresent(),
                "getTable() called on a table handle that has no metastore table object; "
                        + "this is likely because it is called on the worker.");
        return table.get();
    }

    public HoodieTableMetaClient getMetaClient()
    {
        checkArgument(lazyMetaClient.isPresent(),
                "getMetaClient() called on a table handle that has no Hudi meta-client; "
                        + "this is likely because it is called on the worker.");
        return lazyMetaClient.get().get();
    }

    @JsonProperty
    public String getLatestCommitTime()
    {
        return lazyLatestCommitTime.get();
    }

    @JsonProperty
    public String getSchemaName()
    {
        return schemaName;
    }

    @JsonProperty
    public String getTableName()
    {
        return tableName;
    }

    @JsonProperty
    public String getBasePath()
    {
        return basePath;
    }

    @JsonProperty
    public HoodieTableType getTableType()
    {
        return tableType;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getPartitionPredicates()
    {
        return partitionPredicates;
    }

    @JsonProperty
    public List<HiveColumnHandle> getPartitionColumns()
    {
        return partitionColumns;
    }

    @JsonProperty
    public String getTableSchemaStr()
    {
        return hudiTableSchema
                .map(Lazy::get)
                .map(HoodieSchema::toString)
                .orElse("");
    }

    @JsonIgnore
    public HoodieSchema getTableSchema()
    {
        return hudiTableSchema.map(Lazy::get).orElse(null);
    }

    @JsonProperty
    public Map<String, String> getTableConfig()
    {
        return lazyTableConfig.get();
    }

    @JsonProperty
    public Optional<HudiCommittedInstants> getCommittedInstants()
    {
        return lazyCommittedInstants.get();
    }

    /**
     * The table state the file group reader needs, built from the table config and committed instants the coordinator
     * captured, so reading a file group does not load the table metadata from storage.
     */
    @JsonIgnore
    public FileGroupReaderTableState getFileGroupReaderTableState()
    {
        return lazyFileGroupReaderTableState.get();
    }

    // do not serialize constraint columns as they are not needed on workers
    @JsonIgnore
    public Set<HiveColumnHandle> getConstraintColumns()
    {
        return constraintColumns;
    }

    @JsonProperty
    public TupleDomain<HiveColumnHandle> getRegularPredicates()
    {
        return regularPredicates;
    }

    @JsonProperty
    public OptionalLong getLimit()
    {
        return limit;
    }

    /**
     * Columns that must be read for the file group reader to merge correctly: the ordering columns, plus any
     * mandatory fields declared by a configured custom record merger. See
     * {@link HudiUtil#getMergeRequiredColumnHandles}.
     */
    @JsonProperty
    public List<HiveColumnHandle> getMergeRequiredColumns()
    {
        return lazyMergeRequiredColumns.get();
    }

    public SchemaTableName getSchemaTableName()
    {
        return schemaTableName(schemaName, tableName);
    }

    HudiTableHandle applyPredicates(
            Set<HiveColumnHandle> constraintColumns,
            TupleDomain<HiveColumnHandle> partitionTupleDomain,
            TupleDomain<HiveColumnHandle> regularTupleDomain)
    {
        return new HudiTableHandle(
                table,
                lazyMetaClient,
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                lazyMergeRequiredColumns,
                constraintColumns,
                partitionPredicates.intersect(partitionTupleDomain),
                regularPredicates.intersect(regularTupleDomain),
                limit,
                hudiTableSchema,
                this::getLatestCommitTime,
                lazyTableConfig,
                lazyCommittedInstants);
    }

    HudiTableHandle withLimit(long newLimit)
    {
        return new HudiTableHandle(
                table,
                lazyMetaClient,
                schemaName,
                tableName,
                basePath,
                tableType,
                partitionColumns,
                lazyMergeRequiredColumns,
                constraintColumns,
                partitionPredicates,
                regularPredicates,
                OptionalLong.of(newLimit),
                hudiTableSchema,
                this::getLatestCommitTime,
                lazyTableConfig,
                lazyCommittedInstants);
    }

    @Override
    public String toString()
    {
        return getSchemaTableName().toString();
    }
}
