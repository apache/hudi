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

import io.trino.filesystem.Location;
import io.trino.filesystem.TrinoInput;
import io.trino.filesystem.TrinoInputFile;
import io.trino.filesystem.TrinoInputStream;
import io.trino.spi.metrics.Metrics;

import java.io.EOFException;
import java.io.IOException;
import java.time.Instant;

import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.min;
import static java.util.Objects.checkFromIndexSize;
import static java.util.Objects.requireNonNull;

/**
 * A {@link TrinoInputFile} over the byte range {@code [startOffset, startOffset + length)} of another file, presented
 * as a file of its own that starts at position 0. A parquet data block in a Hudi log file is a complete parquet file
 * embedded at such a range, so this lets the parquet reader open the block in place.
 */
public final class InlineTrinoInputFile
        implements TrinoInputFile
{
    private final TrinoInputFile outerFile;
    private final long startOffset;
    private final long length;

    public InlineTrinoInputFile(TrinoInputFile outerFile, long startOffset, long length)
    {
        this.outerFile = requireNonNull(outerFile, "outerFile is null");
        checkArgument(startOffset >= 0, "startOffset is negative: %s", startOffset);
        checkArgument(length >= 0, "length is negative: %s", length);
        this.startOffset = startOffset;
        this.length = length;
    }

    @Override
    public TrinoInput newInput()
            throws IOException
    {
        return new InlineTrinoInput(outerFile.newInput());
    }

    @Override
    public TrinoInputStream newStream()
            throws IOException
    {
        TrinoInputStream stream = outerFile.newStream();
        try {
            stream.seek(startOffset);
        }
        catch (IOException | RuntimeException e) {
            try {
                stream.close();
            }
            catch (IOException closeException) {
                e.addSuppressed(closeException);
            }
            throw e;
        }
        return new InlineTrinoInputStream(stream);
    }

    @Override
    public long length()
    {
        return length;
    }

    @Override
    public Instant lastModified()
            throws IOException
    {
        return outerFile.lastModified();
    }

    @Override
    public boolean exists()
            throws IOException
    {
        return outerFile.exists();
    }

    /**
     * The outer file's location, so file system caches stay keyed on the bytes actually read. Every block of a log
     * file shares it, so it must not key anything per block, such as a parquet footer cache.
     */
    @Override
    public Location location()
    {
        return outerFile.location();
    }

    @Override
    public String toString()
    {
        return "%s[%s, %s)".formatted(outerFile.location(), startOffset, startOffset + length);
    }

    private final class InlineTrinoInput
            implements TrinoInput
    {
        private final TrinoInput outerInput;

        private InlineTrinoInput(TrinoInput outerInput)
        {
            this.outerInput = outerInput;
        }

        @Override
        public void readFully(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
            if (position < 0 || position > length - bufferLength) {
                throw new EOFException("Read of %s bytes at position %s is outside inline content of length %s in %s"
                        .formatted(bufferLength, position, length, InlineTrinoInputFile.this));
            }
            outerInput.readFully(startOffset + position, buffer, bufferOffset, bufferLength);
        }

        @Override
        public int readTail(byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            checkFromIndexSize(bufferOffset, bufferLength, buffer.length);
            int readLength = (int) min(bufferLength, length);
            outerInput.readFully(startOffset + length - readLength, buffer, bufferOffset, readLength);
            return readLength;
        }

        @Override
        public Metrics getMetrics()
        {
            return outerInput.getMetrics();
        }

        @Override
        public void close()
                throws IOException
        {
            outerInput.close();
        }
    }

    private final class InlineTrinoInputStream
            extends TrinoInputStream
    {
        private final TrinoInputStream outerStream;
        private long position;

        private InlineTrinoInputStream(TrinoInputStream outerStream)
        {
            this.outerStream = outerStream;
        }

        @Override
        public long getPosition()
        {
            return position;
        }

        @Override
        public void seek(long newPosition)
                throws IOException
        {
            if (newPosition < 0 || newPosition > length) {
                throw new IOException("Cannot seek to %s outside inline content of length %s in %s"
                        .formatted(newPosition, length, InlineTrinoInputFile.this));
            }
            outerStream.seek(startOffset + newPosition);
            position = newPosition;
        }

        @Override
        public int read()
                throws IOException
        {
            if (position >= length) {
                return -1;
            }
            int value = outerStream.read();
            if (value >= 0) {
                position++;
            }
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int bufferLength)
                throws IOException
        {
            checkFromIndexSize(offset, bufferLength, buffer.length);
            if (bufferLength == 0) {
                return 0;
            }
            if (position >= length) {
                return -1;
            }
            int read = outerStream.read(buffer, offset, (int) min(bufferLength, length - position));
            if (read > 0) {
                position += read;
            }
            return read;
        }

        @Override
        public long skip(long n)
                throws IOException
        {
            if (n <= 0) {
                return 0;
            }
            long skip = min(n, length - position);
            seek(position + skip);
            return skip;
        }

        @Override
        public int available()
                throws IOException
        {
            return (int) min(outerStream.available(), length - position);
        }

        @Override
        public void close()
                throws IOException
        {
            outerStream.close();
        }
    }
}
