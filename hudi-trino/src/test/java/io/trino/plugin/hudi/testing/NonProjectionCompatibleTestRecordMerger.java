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

/**
 * Test-only merger that is identical to {@link KeyBasedTestRecordMerger} except that it reports
 * {@code isProjectionCompatible() == false}, which makes the file-group reader request the FULL table schema
 * instead of the connector's projection.
 * <p>
 * The connector expands the read projection to the full table schema for such mergers, so narrow queries
 * still merge correctly. This merger exists purely to exercise that full-schema path; it shares
 * {@link KeyBasedTestRecordMerger#MERGE_STRATEGY_ID} so it resolves against the same test table.
 */
public class NonProjectionCompatibleTestRecordMerger
        extends KeyBasedTestRecordMerger
{
    @Override
    public boolean isProjectionCompatible()
    {
        return false;
    }
}
