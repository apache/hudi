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
import io.trino.plugin.hive.HiveColumnHandle;
import io.trino.plugin.hudi.util.PrefilledColumnValues;
import io.trino.spi.Page;
import io.trino.spi.block.Block;
import io.trino.spi.connector.ConnectorPageSource;
import io.trino.spi.connector.SourcePage;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * This page source is for reading data columns in the parquet format.
 * This page source also avoids costly avro IndexRecord serialization.
 */
public class HudiBaseFileOnlyPageSource
        implements ConnectorPageSource
{
    private final ConnectorPageSource dataPageSource;
    private final List<HiveColumnHandle> allOutputColumns;
    private final PrefilledColumnValues prefilledColumnValues;
    // Maps output channel to physical source channel, or -1 if prefilled
    private final int[] physicalSourceChannelMap;

    public HudiBaseFileOnlyPageSource(
            ConnectorPageSource dataPageSource,
            List<HiveColumnHandle> allOutputColumns,
            // Columns provided by dataPageSource
            List<HiveColumnHandle> dataColumns,
            // Per-split constant values for columns not present in the data file, such as partition
            // columns and Trino's hidden metadata columns, e.g. file size (not hudi metadata)
            PrefilledColumnValues prefilledColumnValues)
    {
        this.dataPageSource = requireNonNull(dataPageSource, "dataPageSource is null");
        this.allOutputColumns = ImmutableList.copyOf(requireNonNull(allOutputColumns, "allOutputColumns is null"));
        this.prefilledColumnValues = requireNonNull(prefilledColumnValues, "prefilledColumnValues is null");

        // Create a mapping from the channel index in the output page to the channel index in the physicalDataPageSource's page
        this.physicalSourceChannelMap = new int[allOutputColumns.size()];
        Map<String, Integer> physicalColumnNameToChannel = new HashMap<>();
        for (int i = 0; i < dataColumns.size(); i++) {
            physicalColumnNameToChannel.put(dataColumns.get(i).getName().toLowerCase(Locale.ENGLISH), i);
        }

        for (int i = 0; i < allOutputColumns.size(); i++) {
            this.physicalSourceChannelMap[i] = physicalColumnNameToChannel.getOrDefault(allOutputColumns.get(i).getName().toLowerCase(Locale.ENGLISH), -1);
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return dataPageSource.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return dataPageSource.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return dataPageSource.isFinished();
    }

    @Override
    public SourcePage getNextSourcePage()
    {
        SourcePage physicalSourcePage = dataPageSource.getNextSourcePage();
        if (physicalSourcePage == null) {
            return null;
        }

        int positionCount = physicalSourcePage.getPositionCount();
        if (allOutputColumns.isEmpty()) {
            // Forward the zero-block page so positionCount survives -- new Page(new Block[0]) would infer positionCount=0.
            return physicalSourcePage;
        }

        Block[] outputBlocks = new Block[allOutputColumns.size()];
        for (int i = 0; i < allOutputColumns.size(); i++) {
            HiveColumnHandle outputColumn = allOutputColumns.get(i);
            if (physicalSourceChannelMap[i] != -1) {
                outputBlocks[i] = physicalSourcePage.getBlock(physicalSourceChannelMap[i]);
            }
            else {
                // Column is not in the data file; fill with the split's constant value
                outputBlocks[i] = prefilledColumnValues.toRleBlock(outputColumn, positionCount);
            }
        }
        return SourcePage.create(new Page(outputBlocks));
    }

    @Override
    public long getMemoryUsage()
    {
        return dataPageSource.getMemoryUsage();
    }

    @Override
    public void close()
            throws IOException
    {
        dataPageSource.close();
    }
}
