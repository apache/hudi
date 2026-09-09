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
import io.trino.plugin.hive.parquet.ParquetReaderConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.testing.TestingConnectorSession;
import org.junit.jupiter.api.Test;

import static io.trino.plugin.hudi.HudiSessionProperties.getColumnsToHide;
import static io.trino.plugin.hudi.HudiSessionProperties.getRecordMergerImpls;
import static io.trino.plugin.hudi.HudiSessionProperties.getTargetSplitSize;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestHudiSessionProperties
{
    @Test
    public void testSessionPropertyColumnsToHide()
    {
        HudiConfig config = new HudiConfig()
                .setColumnsToHide(ImmutableList.of("col1", "col2"));
        HudiSessionProperties sessionProperties = new HudiSessionProperties(config, new ParquetReaderConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
        assertThat(getColumnsToHide(session))
                .containsExactlyInAnyOrderElementsOf(ImmutableList.of("col1", "col2"));
    }

    @Test
    public void testSessionPropertyRecordMergerImpls()
    {
        HudiConfig config = new HudiConfig()
                .setRecordMergerImpls(ImmutableList.of("com.example.MergerOne", "com.example.MergerTwo"));
        HudiSessionProperties sessionProperties = new HudiSessionProperties(config, new ParquetReaderConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .build();
        assertThat(getRecordMergerImpls(session))
                .containsExactly("com.example.MergerOne", "com.example.MergerTwo");
    }

    @Test
    public void testSessionPropertyTargetSplitSizeRejectsZero()
    {
        // A zero target split size would make split generation loop forever, so reject it when the property is read
        HudiSessionProperties sessionProperties = new HudiSessionProperties(new HudiConfig(), new ParquetReaderConfig());
        ConnectorSession session = TestingConnectorSession.builder()
                .setPropertyMetadata(sessionProperties.getSessionProperties())
                .setPropertyValues(ImmutableMap.of("target_split_size", "0B"))
                .build();
        assertThatThrownBy(() -> getTargetSplitSize(session))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("target_split_size must be at least 1B: 0B");
    }
}
