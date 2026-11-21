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
package io.trino.plugin.hudi.io;

import io.airlift.slice.Slices;
import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.filesystem.memory.MemoryInputFile;
import org.junit.jupiter.api.Test;

import java.io.EOFException;
import java.io.IOException;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class TestInlineTrinoInputFile
{
    private static final String OUTER_CONTENT = "0123456789ABCDEFGHIJ";
    private static final Location LOCATION = Location.of("memory:///table/.f1_20240101.log.1_0-1-1");

    // The inline content is "56789ABCDE"
    private static final long START_OFFSET = 5;
    private static final long LENGTH = 10;

    @Test
    void testLengthAndLocation()
            throws IOException
    {
        TrinoInputFile file = inlineFile();
        assertThat(file.length()).isEqualTo(LENGTH);
        assertThat(file.location()).isEqualTo(LOCATION);
        assertThat(file.exists()).isTrue();
    }

    @Test
    void testReadFullyIsRelativeToStartOffset()
            throws IOException
    {
        try (TrinoInput input = inlineFile().newInput()) {
            assertThat(input.readFully(0, 3).toStringUtf8()).isEqualTo("567");
            assertThat(input.readFully(7, 3).toStringUtf8()).isEqualTo("CDE");

            byte[] buffer = new byte[6];
            input.readFully(2, buffer, 1, 4);
            assertThat(new String(buffer, 1, 4, UTF_8)).isEqualTo("789A");
        }
    }

    @Test
    void testReadFullyPastTheInlineContentFails()
            throws IOException
    {
        try (TrinoInput input = inlineFile().newInput()) {
            // Bytes exist in the outer file there, but they belong to the next block
            assertThatThrownBy(() -> input.readFully(8, 3))
                    .isInstanceOf(EOFException.class)
                    .hasMessageContaining("outside inline content of length 10");
            assertThatThrownBy(() -> input.readFully(-1, 2))
                    .isInstanceOf(EOFException.class);
        }
    }

    @Test
    void testReadTailEndsAtTheInlineContent()
            throws IOException
    {
        try (TrinoInput input = inlineFile().newInput()) {
            assertThat(input.readTail(4).toStringUtf8()).isEqualTo("BCDE");
            // A tail longer than the content returns the whole content, as for a short file
            assertThat(input.readTail(64).toStringUtf8()).isEqualTo("56789ABCDE");

            byte[] buffer = new byte[5];
            assertThat(input.readTail(buffer, 2, 3)).isEqualTo(3);
            assertThat(new String(buffer, 2, 3, UTF_8)).isEqualTo("CDE");
        }
    }

    @Test
    void testStreamIsBoundedByTheInlineContent()
            throws IOException
    {
        try (TrinoInputStream stream = inlineFile().newStream()) {
            assertThat(stream.getPosition()).isEqualTo(0);
            assertThat(stream.read()).isEqualTo('5');
            assertThat(new String(stream.readAllBytes(), UTF_8)).isEqualTo("6789ABCDE");
            assertThat(stream.getPosition()).isEqualTo(LENGTH);
            assertThat(stream.read()).isEqualTo(-1);

            stream.seek(7);
            assertThat(new String(stream.readNBytes(10), UTF_8)).isEqualTo("CDE");

            stream.seek(2);
            assertThat(stream.skip(3)).isEqualTo(3);
            assertThat(stream.read()).isEqualTo('A');
            assertThat(stream.skip(100)).isEqualTo(4);
            assertThat(stream.getPosition()).isEqualTo(LENGTH);
            assertThatThrownBy(() -> stream.seek(LENGTH + 1))
                    .isInstanceOf(IOException.class)
                    .hasMessageContaining("outside inline content of length 10");
        }
    }

    @Test
    void testContentAtTheStartOfTheFile()
            throws IOException
    {
        TrinoInputFile file = new InlineTrinoInputFile(outerFile(), 0, 4);
        try (TrinoInput input = file.newInput()) {
            assertThat(input.readTail(4).toStringUtf8()).isEqualTo("0123");
        }
    }

    private static TrinoInputFile inlineFile()
    {
        return new InlineTrinoInputFile(outerFile(), START_OFFSET, LENGTH);
    }

    private static TrinoInputFile outerFile()
    {
        return new MemoryInputFile(LOCATION, Slices.utf8Slice(OUTER_CONTENT));
    }
}
