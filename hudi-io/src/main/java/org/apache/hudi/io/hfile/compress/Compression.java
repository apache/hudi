/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.io.hfile.compress;

import org.apache.hudi.io.compress.CodecPool;
import org.apache.hudi.io.compress.CompressionCodec;
import org.apache.hudi.io.compress.CompressionInputStream;
import org.apache.hudi.io.compress.CompressionOutputStream;
import org.apache.hudi.io.compress.Compressor;
import org.apache.hudi.io.compress.Decompressor;
import org.apache.hudi.io.compress.DoNotPool;
import org.apache.hudi.io.compress.GzipCodec;
import org.apache.hudi.io.compress.ReusableStreamGzipCodec;
import org.apache.hudi.io.hfile.ByteUtils;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 * Compression related stuff.
 * Copied from hadoop-3315 tfile.
 * <p>
 * This class is adapted from
 * {@code org.apache.hadoop.hbase.io.compress.Compression}
 */
public final class Compression {
  private static final Logger LOG = LoggerFactory.getLogger(Compression.class);

  /**
   * Prevent the instantiation of class.
   */
  private Compression() {
    super();
  }

  static class FinishOnFlushCompressionStream extends FilterOutputStream {
    public FinishOnFlushCompressionStream(CompressionOutputStream cout) {
      super(cout);
    }

    @Override
    public void write(byte b[], int off, int len) throws IOException {
      out.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
      CompressionOutputStream cout = (CompressionOutputStream) out;
      cout.finish();
      cout.flush();
      cout.resetState();
    }
  }

  /**
   * Returns the classloader to load the Codec class from.
   */
  private static ClassLoader getClassLoaderForCodec() {
    ClassLoader cl = Thread.currentThread().getContextClassLoader();
    if (cl == null) {
      cl = Compression.class.getClassLoader();
    }
    if (cl == null) {
      cl = ClassLoader.getSystemClassLoader();
    }
    if (cl == null) {
      throw new RuntimeException("A ClassLoader to load the Codec could not be determined");
    }
    return cl;
  }

  /**
   * Compression algorithms. The ordinal of these cannot change or else you
   * risk breaking all existing HFiles out there.  Even the ones that are
   * not compressed! (They use the NONE algorithm)
   */
  public static enum Algorithm {
    // LZO is GPL and requires extra install to setup. See
    // https://stackoverflow.com/questions/23441142/class-com-hadoop-compression-lzo-lzocodec-not-found-for-spark-on-cdh-5
    LZO("lzo") {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec lzoCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (lzoCodec == null) {
          synchronized (lock) {
            if (lzoCodec == null) {
              lzoCodec = buildCodec(conf);
            }
          }
        }
        return lzoCodec;
      }

      private CompressionCodec buildCodec(Configuration conf) {
        try {
          Class<?> externalCodec =
              getClassLoaderForCodec().loadClass("com.hadoop.compression.lzo.LzoCodec");
          return (CompressionCodec) ReflectionUtils.newInstance(externalCodec,
              new Configuration(conf));
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    },
    GZ("gz") {
      private volatile transient GzipCodec codec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (codec == null) {
          synchronized (lock) {
            if (codec == null) {
              codec = buildCodec(conf);
            }
          }
        }

        return codec;
      }

      private GzipCodec buildCodec(Configuration conf) {
        GzipCodec gzcodec = new ReusableStreamGzipCodec();
        gzcodec.setConf(new Configuration(conf));
        return gzcodec;
      }
    },

    NONE("none") {
      @Override
      CompressionCodec getCodec(Configuration conf) {
        return null;
      }

      @Override
      public synchronized InputStream createDecompressionStream(
          InputStream downStream, Decompressor decompressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedInputStream(downStream, downStreamBufferSize);
        }
        return downStream;
      }

      @Override
      public synchronized OutputStream createCompressionStream(
          OutputStream downStream, Compressor compressor,
          int downStreamBufferSize) throws IOException {
        if (downStreamBufferSize > 0) {
          return new BufferedOutputStream(downStream, downStreamBufferSize);
        }

        return downStream;
      }
    },
    SNAPPY("snappy") {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec snappyCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (snappyCodec == null) {
          synchronized (lock) {
            if (snappyCodec == null) {
              snappyCodec = buildCodec(conf);
            }
          }
        }
        return snappyCodec;
      }

      private CompressionCodec buildCodec(Configuration conf) {
        try {
          Class<?> externalCodec =
              getClassLoaderForCodec().loadClass("org.apache.hadoop.io.compress.SnappyCodec");
          return (CompressionCodec) ReflectionUtils.newInstance(externalCodec, conf);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    },
    LZ4("lz4") {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec lz4Codec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (lz4Codec == null) {
          synchronized (lock) {
            if (lz4Codec == null) {
              lz4Codec = buildCodec(conf);
            }
          }
        }
        return lz4Codec;
      }

      private CompressionCodec buildCodec(Configuration conf) {
        try {
          Class<?> externalCodec =
              getClassLoaderForCodec().loadClass("org.apache.hadoop.io.compress.Lz4Codec");
          return (CompressionCodec) ReflectionUtils.newInstance(externalCodec, conf);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    },
    BZIP2("bzip2") {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec bzipCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (bzipCodec == null) {
          synchronized (lock) {
            if (bzipCodec == null) {
              bzipCodec = buildCodec(conf);
            }
          }
        }
        return bzipCodec;
      }

      private CompressionCodec buildCodec(Configuration conf) {
        try {
          Class<?> externalCodec =
              getClassLoaderForCodec().loadClass("org.apache.hadoop.io.compress.BZip2Codec");
          return (CompressionCodec) ReflectionUtils.newInstance(externalCodec, conf);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    },
    ZSTD("zstd") {
      // Use base type to avoid compile-time dependencies.
      private volatile transient CompressionCodec zStandardCodec;
      private final transient Object lock = new Object();

      @Override
      CompressionCodec getCodec(Configuration conf) {
        if (zStandardCodec == null) {
          synchronized (lock) {
            if (zStandardCodec == null) {
              zStandardCodec = buildCodec(conf);
            }
          }
        }
        return zStandardCodec;
      }

      private CompressionCodec buildCodec(Configuration conf) {
        try {
          Class<?> externalCodec =
              getClassLoaderForCodec().loadClass("org.apache.hadoop.io.compress.ZStandardCodec");
          return (CompressionCodec) ReflectionUtils.newInstance(externalCodec, conf);
        } catch (ClassNotFoundException e) {
          throw new RuntimeException(e);
        }
      }
    };

    private final Configuration conf;
    private final String compressName;
    /**
     * data input buffer size to absorb small reads from application.
     */
    private static final int DATA_IBUF_SIZE = 1 * 1024;
    /**
     * data output buffer size to absorb small writes from application.
     */
    private static final int DATA_OBUF_SIZE = 4 * 1024;

    Algorithm(String name) {
      this.conf = new Configuration();
      this.conf.setBoolean("io.native.lib.available", true);
      this.compressName = name;
    }

    abstract CompressionCodec getCodec(Configuration conf);

    public InputStream createDecompressionStream(
        InputStream downStream, Decompressor decompressor,
        int downStreamBufferSize) throws IOException {
      CompressionCodec codec = getCodec(conf);
      // Set the internal buffer size to read from down stream.
      if (downStreamBufferSize > 0) {
        ((Configurable) codec).getConf().setInt("io.file.buffer.size",
            downStreamBufferSize);
      }
      CompressionInputStream cis =
          codec.createInputStream(downStream, decompressor);
      BufferedInputStream bis2 = new BufferedInputStream(cis, DATA_IBUF_SIZE);
      return bis2;

    }

    public OutputStream createCompressionStream(
        OutputStream downStream, Compressor compressor, int downStreamBufferSize)
        throws IOException {
      OutputStream bos1 = null;
      if (downStreamBufferSize > 0) {
        bos1 = new BufferedOutputStream(downStream, downStreamBufferSize);
      } else {
        bos1 = downStream;
      }
      CompressionOutputStream cos =
          createPlainCompressionStream(bos1, compressor);
      BufferedOutputStream bos2 =
          new BufferedOutputStream(new FinishOnFlushCompressionStream(cos),
              DATA_OBUF_SIZE);
      return bos2;
    }

    /**
     * Creates a compression stream without any additional wrapping into
     * buffering streams.
     */
    public CompressionOutputStream createPlainCompressionStream(
        OutputStream downStream, Compressor compressor) throws IOException {
      CompressionCodec codec = getCodec(conf);
      ((Configurable) codec).getConf().setInt("io.file.buffer.size", 32 * 1024);
      return codec.createOutputStream(downStream, compressor);
    }

    public Compressor getCompressor() {
      CompressionCodec codec = getCodec(conf);
      if (codec != null) {
        Compressor compressor = CodecPool.getCompressor(codec);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Retrieved compressor " + compressor + " from pool.");
        }
        if (compressor != null) {
          if (compressor.finished()) {
            // Somebody returns the compressor to CodecPool but is still using it.
            LOG.warn("Compressor obtained from CodecPool is already finished()");
          }
          compressor.reset();
        }
        return compressor;
      }
      return null;
    }

    public void returnCompressor(Compressor compressor) {
      if (compressor != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Returning compressor " + compressor + " to pool.");
        }
        CodecPool.returnCompressor(compressor);
      }
    }

    public Decompressor getDecompressor() {
      CompressionCodec codec = getCodec(conf);
      if (codec != null) {
        Decompressor decompressor = CodecPool.getDecompressor(codec);
        if (LOG.isTraceEnabled()) {
          LOG.trace("Retrieved decompressor " + decompressor + " from pool.");
        }
        if (decompressor != null) {
          if (decompressor.finished()) {
            // Somebody returns the decompressor to CodecPool but is still using it.
            LOG.warn("Deompressor obtained from CodecPool is already finished()");
          }
          decompressor.reset();
        }
        return decompressor;
      }

      return null;
    }

    public void returnDecompressor(Decompressor decompressor) {
      if (decompressor != null) {
        if (LOG.isTraceEnabled()) {
          LOG.trace("Returning decompressor " + decompressor + " to pool.");
        }
        CodecPool.returnDecompressor(decompressor);
        if (decompressor.getClass().isAnnotationPresent(DoNotPool.class)) {
          if (LOG.isTraceEnabled()) {
            LOG.trace("Ending decompressor " + decompressor);
          }
          decompressor.end();
        }
      }
    }

    public String getName() {
      return compressName;
    }
  }

  public static Algorithm getCompressionAlgorithmByName(String compressName) {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    for (Algorithm a : algos) {
      if (a.getName().equals(compressName)) {
        return a;
      }
    }

    throw new IllegalArgumentException("Unsupported compression algorithm name: " + compressName);
  }

  /**
   * Get names of supported compression algorithms.
   *
   * @return Array of strings, each represents a supported compression
   * algorithm. Currently, the following compression algorithms are supported.
   */
  public static String[] getSupportedAlgorithms() {
    Algorithm[] algos = Algorithm.class.getEnumConstants();

    String[] ret = new String[algos.length];
    int i = 0;
    for (Algorithm a : algos) {
      ret[i++] = a.getName();
    }

    return ret;
  }

  /**
   * Decompresses data from the given stream using the configured compression algorithm. It will
   * throw an exception if the dest buffer does not have enough space to hold the decompressed data.
   *
   * @param dest                  the output buffer
   * @param bufferedBoundedStream a stream to read compressed data from, bounded to the exact amount
   *                              of compressed data
   * @param uncompressedSize      uncompressed data size, header not included
   * @param compressAlgo          compression algorithm used
   * @throws IOException if any IO error happen
   */
  public static void decompress(ByteBuffer dest, InputStream bufferedBoundedStream,
                                int uncompressedSize, Compression.Algorithm compressAlgo) throws IOException {
    if (dest.remaining() < uncompressedSize) {
      throw new IllegalArgumentException("Output buffer does not have enough space to hold "
          + uncompressedSize + " decompressed bytes, available: " + dest.remaining());
    }

    Decompressor decompressor = null;
    try {
      decompressor = compressAlgo.getDecompressor();
      try (InputStream is =
               compressAlgo.createDecompressionStream(bufferedBoundedStream, decompressor, 0)) {
        ByteUtils.readFullyWithHeapBuffer(is, dest, uncompressedSize);
      }
    } finally {
      if (decompressor != null) {
        compressAlgo.returnDecompressor(decompressor);
      }
    }
  }
}
