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
package io.trino.plugin.hudi.util;

import io.trino.spi.block.BlockBuilder;
import io.trino.spi.type.CharType;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.List;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

public class TestHudiAvroCharSerialization
{
    @ParameterizedTest
    @MethodSource("charValues")
    public void testCharPreservesLeadingSpaces(String input, String expected)
    {
        CharType type = CharType.createCharType(5);
        for (Object value : List.of(input, new Utf8(input))) {
            BlockBuilder builder = type.createBlockBuilder(null, 1);
            HudiAvroSerializer.appendTo(type, value, builder);
            assertThat(type.getSlice(builder.build(), 0).toStringUtf8()).isEqualTo(expected);
        }
    }

    private static Stream<Arguments> charValues()
    {
        return Stream.of(
                Arguments.of(" ab  ", " ab"),
                Arguments.of("     ", ""),
                Arguments.of("\tab\t ", "\tab\t"),
                Arguments.of(" a😀  ", " a😀"),
                Arguments.of(" abcdef", " abcd"));
    }
}
