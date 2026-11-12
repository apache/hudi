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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.table.HoodieTableConfig;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.HoodieTableVersion;
import org.apache.hudi.common.table.read.CommittedInstants;
import org.apache.hudi.common.table.read.TableState;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.storage.StoragePath;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;
import static org.apache.hudi.common.table.HoodieTableConfig.CREATE_SCHEMA;

/**
 * The table-level state the merge read path of a split needs, captured once on the coordinator from the meta client
 * it already holds and shipped to the workers inside {@link HudiTableHandle}. Without it every split with log files
 * built its own {@link HoodieTableMetaClient}, which reads {@code hoodie.properties} (and the index definitions, when
 * the table has any), and for tables before version 8 also loaded the commits timeline to validate log blocks.
 * <p>
 * This is the JSON form of hudi-common's {@link TableState}: the persisted table config, plus, for tables before
 * version 8 only, the committed instants that log blocks are checked against. Capturing them where the file slices
 * and the latest commit time are resolved means every split of a query validates against that same timeline
 * snapshot. Schema-on-read history is not carried: the connector never hands the file group reader an internal
 * schema, so the reader never asks for it.
 *
 * @param tableConfig the persisted table config, without {@link HoodieTableConfig#CREATE_SCHEMA}
 * @param committedInstants the committed instants, present only for tables before version 8
 */
public record HudiTableState(Map<String, String> tableConfig, Optional<CommittedInstantTimes> committedInstants)
{
    public HudiTableState
    {
        tableConfig = ImmutableMap.copyOf(requireNonNull(tableConfig, "tableConfig is null"));
        requireNonNull(committedInstants, "committedInstants is null");
    }

    /**
     * Captures the state from a coordinator meta client. Loads the active timeline for tables before version 8, which
     * the coordinator loads anyway to list the file slices and resolve the latest commit time.
     */
    public static HudiTableState capture(HoodieTableMetaClient metaClient)
    {
        HoodieTableConfig tableConfig = metaClient.getTableConfig();
        TypedProperties props = tableConfig.getProps();
        ImmutableMap.Builder<String, String> transportedConfig = ImmutableMap.builder();
        for (String key : props.stringPropertyNames()) {
            // The create schema only serves as a fallback for resolving the table schema, which the handle already
            // carries resolved; dropping it keeps a second copy of the schema out of the serialized handle.
            if (!CREATE_SCHEMA.key().equals(key)) {
                transportedConfig.put(key, props.getProperty(key));
            }
        }

        Optional<CommittedInstantTimes> committedInstants = Optional.empty();
        if (tableConfig.getTableVersion().lesserThan(HoodieTableVersion.EIGHT)) {
            committedInstants = Optional.of(CommittedInstantTimes.from(CommittedInstants.fromCommitsTimeline(metaClient.getCommitsTimeline())));
        }
        return new HudiTableState(transportedConfig.buildOrThrow(), committedInstants);
    }

    /**
     * Builds a new table config on every call, so each split's file group reader gets a private copy, as it did when
     * each split loaded its own.
     */
    public HoodieTableConfig toTableConfig()
    {
        HoodieTableConfig config = new HoodieTableConfig();
        config.getProps().putAll(tableConfig);
        return config;
    }

    /**
     * The state for the file group reader of one split, around the table config built for that split.
     */
    public TableState toTableState(String basePath, HoodieTableConfig config)
    {
        return TableState.of(
                new StoragePath(basePath),
                config,
                Option.fromJavaOptional(committedInstants.map(CommittedInstantTimes::toCommittedInstants)),
                Option.empty());
    }

    /**
     * The instant times of a {@link CommittedInstants}, in a form JSON can carry.
     */
    public record CommittedInstantTimes(List<String> completed, List<String> inflight, Optional<String> timelineStart)
    {
        public CommittedInstantTimes
        {
            completed = ImmutableList.copyOf(requireNonNull(completed, "completed is null"));
            inflight = ImmutableList.copyOf(requireNonNull(inflight, "inflight is null"));
            requireNonNull(timelineStart, "timelineStart is null");
        }

        static CommittedInstantTimes from(CommittedInstants committedInstants)
        {
            return new CommittedInstantTimes(
                    ImmutableList.sortedCopyOf(committedInstants.getCompletedInstants()),
                    ImmutableList.sortedCopyOf(committedInstants.getInflightInstants()),
                    Optional.ofNullable(committedInstants.getTimelineStartInstant().orElse(null)));
        }

        CommittedInstants toCommittedInstants()
        {
            return CommittedInstants.of(completed, inflight, Option.fromJavaOptional(timelineStart));
        }
    }
}
