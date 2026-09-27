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
import org.apache.hudi.common.table.read.CommittedInstants;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * The committed instants that the file group reader checks log blocks against for tables before version 8, as plain
 * instant times so the coordinator can ship them to workers on the table handle.
 */
public record HudiCommittedInstants(List<String> completedInstants, List<String> inflightInstants, Optional<String> timelineStartInstant)
{
    public HudiCommittedInstants
    {
        completedInstants = ImmutableList.copyOf(requireNonNull(completedInstants, "completedInstants is null"));
        inflightInstants = ImmutableList.copyOf(requireNonNull(inflightInstants, "inflightInstants is null"));
        requireNonNull(timelineStartInstant, "timelineStartInstant is null");
    }

    /**
     * Captures the instants of the commits timeline up to the latest commit a query reads. The reader skips data and
     * delete blocks written after that commit before checking whether they are committed, so later instants are not
     * needed.
     */
    public static HudiCommittedInstants capture(HoodieTimeline commitsTimeline, String latestCommitTime)
    {
        CommittedInstants committed = CommittedInstants.fromCommitsTimeline(commitsTimeline.findInstantsBeforeOrEquals(latestCommitTime));
        return new HudiCommittedInstants(
                sorted(committed.getCompletedInstants()),
                sorted(committed.getInflightInstants()),
                committed.getTimelineStartInstant().toJavaOptional());
    }

    public CommittedInstants toCommittedInstants()
    {
        return CommittedInstants.of(completedInstants, inflightInstants, Option.fromJavaOptional(timelineStartInstant));
    }

    private static List<String> sorted(Collection<String> instants)
    {
        return instants.stream().sorted().collect(toImmutableList());
    }
}
