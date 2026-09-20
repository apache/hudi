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
import io.trino.spi.TrinoException;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static io.trino.plugin.hudi.HudiTableProperties.ORDERING_FIELDS_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.PRECOMBINE_FIELD_PROPERTY;
import static io.trino.plugin.hudi.HudiTableProperties.getOrderingFields;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

final class TestHudiTableProperties
{
    @Test
    void testOrderingFieldsIsThePrimaryName()
    {
        assertThat(getOrderingFields(Map.of(ORDERING_FIELDS_PROPERTY, ImmutableList.of("event_time"))))
                .containsExactly("event_time");
    }

    @Test
    void testPrecombineFieldIsAcceptedAsDeprecatedAlias()
    {
        // Spark still accepts preCombineField via HoodieOptionConfig#withAlternatives, and the
        // connector's design discussion used precombine_field in its CREATE TABLE example; both
        // should keep working rather than failing as an unknown property.
        assertThat(getOrderingFields(Map.of(PRECOMBINE_FIELD_PROPERTY, "event_time")))
                .containsExactly("event_time");
    }

    @Test
    void testPrecombineFieldIsLowerCasedLikeOrderingFields()
    {
        assertThat(getOrderingFields(Map.of(PRECOMBINE_FIELD_PROPERTY, "EventTime")))
                .containsExactly("eventtime");
    }

    @Test
    void testSettingBothNamesIsRejected()
    {
        // Both write hoodie.table.ordering.fields, so preferring one silently would hide a conflict.
        assertThatThrownBy(() -> getOrderingFields(ImmutableMap.of(
                ORDERING_FIELDS_PROPERTY, ImmutableList.of("event_time"),
                PRECOMBINE_FIELD_PROPERTY, "other_time")))
                .isInstanceOf(TrinoException.class)
                .hasMessageContaining("Cannot set both")
                .hasMessageContaining("hoodie.table.ordering.fields");
    }

    @Test
    void testAbsentFromBothYieldsEmpty()
    {
        assertThat(getOrderingFields(Map.of())).isEmpty();
    }
}
