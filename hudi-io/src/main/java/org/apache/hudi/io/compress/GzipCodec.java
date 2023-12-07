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

package org.apache.hudi.io.compress;

import org.apache.hudi.io.compress.zlib.BuiltInGzipDecompressor;

import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.zlib.ZlibCompressor;
import org.apache.hadoop.io.compress.zlib.ZlibDecompressor;
import org.apache.hadoop.io.compress.zlib.ZlibFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.zip.GZIPOutputStream;

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_DEFAULT;
import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.IO_FILE_BUFFER_SIZE_KEY;

/**
 * This class creates gzip compressors/decompressors.
 * <p>
 * This class is copied from
 * * {@code org.apache.hadoop.io.compress.GzipCodec}
 */
public class GzipCodec implements Configurable, CompressionCodec {

  /**
   * @see <a href="{@docRoot}/../hadoop-project-dist/hadoop-common/core-default.xml">
   * core-default.xml</a>
   */
  public static final String IO_FILE_BUFFER_SIZE_KEY = "io.file.buffer.size";
  /**
   * Default value for IO_FILE_BUFFER_SIZE_KEY
   */
  public static final int IO_FILE_BUFFER_SIZE_DEFAULT = 4096;

  /**
   * A bridge that wraps around a DeflaterOutputStream to make it
   * a CompressionOutputStream.
   */
  protected static class GzipOutputStream extends CompressorStream {

    private static class ResetableGZIPOutputStream extends GZIPOutputStream {

      public ResetableGZIPOutputStream(OutputStream out) throws IOException {
        super(out);
      }

      public void resetState() throws IOException {
        def.reset();
      }
    }

    public GzipOutputStream(OutputStream out) throws IOException {
      super(new GzipCodec.GzipOutputStream.ResetableGZIPOutputStream(out));
    }

    /**
     * Allow children types to put a different type in here.
     *
     * @param out the Deflater stream to use
     */
    protected GzipOutputStream(CompressorStream out) {
      super(out);
    }

    @Override
    public void close() throws IOException {
      out.close();
    }

    @Override
    public void flush() throws IOException {
      out.flush();
    }

    @Override
    public void write(int b) throws IOException {
      out.write(b);
    }

    @Override
    public void write(byte[] data, int offset, int length)
        throws IOException {
      out.write(data, offset, length);
    }

    @Override
    public void finish() throws IOException {
      ((GzipCodec.GzipOutputStream.ResetableGZIPOutputStream) out).finish();
    }

    @Override
    public void resetState() throws IOException {
      ((GzipCodec.GzipOutputStream.ResetableGZIPOutputStream) out).resetState();
    }
  }

  Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out)
      throws IOException {
    if (!ZlibFactory.isNativeZlibLoaded(conf)) {
      return new GzipCodec.GzipOutputStream(out);
    }
    return CompressionCodec.Util.
        createOutputStreamWithCodecPool(this, conf, out);
  }

  @Override
  public CompressionOutputStream createOutputStream(OutputStream out,
                                                    Compressor compressor)
      throws IOException {
    return (compressor != null) ?
        new CompressorStream(out, compressor,
            conf.getInt(IO_FILE_BUFFER_SIZE_KEY,
                IO_FILE_BUFFER_SIZE_DEFAULT)) :
        createOutputStream(out);
  }

  @Override
  public Compressor createCompressor() {
    //return (ZlibFactory.isNativeZlibLoaded(conf))
    //    ? new GzipCodec.GzipZlibCompressor(conf)
    //    : null;
    return null;
  }

  @Override
  public Class<? extends Compressor> getCompressorType() {
    //return ZlibFactory.isNativeZlibLoaded(conf)
    //    ? GzipCodec.GzipZlibCompressor.class
    //    : null;
    return null;
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in)
      throws IOException {
    return CompressionCodec.Util.
        createInputStreamWithCodecPool(this, conf, in);
  }

  @Override
  public CompressionInputStream createInputStream(InputStream in,
                                                  Decompressor decompressor)
      throws IOException {
    if (decompressor == null) {
      decompressor = createDecompressor();  // always succeeds (or throws)
    }
    return new DecompressorStream(in, decompressor,
        conf.getInt(IO_FILE_BUFFER_SIZE_KEY,
            IO_FILE_BUFFER_SIZE_DEFAULT));
  }

  @Override
  public Decompressor createDecompressor() {
    //return (ZlibFactory.isNativeZlibLoaded(conf))
    //    ? new GzipCodec.GzipZlibDecompressor()
    //    : new BuiltInGzipDecompressor();
    return new BuiltInGzipDecompressor();
  }

  @Override
  public Class<? extends Decompressor> getDecompressorType() {
    //return ZlibFactory.isNativeZlibLoaded(conf)
    //    ? GzipCodec.GzipZlibDecompressor.class
    //    : BuiltInGzipDecompressor.class;
    return BuiltInGzipDecompressor.class;
  }

  @Override
  public String getDefaultExtension() {
    return ".gz";
  }

  static final class GzipZlibCompressor extends ZlibCompressor {
    public GzipZlibCompressor() {
      super(ZlibCompressor.CompressionLevel.DEFAULT_COMPRESSION,
          ZlibCompressor.CompressionStrategy.DEFAULT_STRATEGY,
          ZlibCompressor.CompressionHeader.GZIP_FORMAT, 64 * 1024);
    }

    public GzipZlibCompressor(Configuration conf) {
      super(ZlibFactory.getCompressionLevel(conf),
          ZlibFactory.getCompressionStrategy(conf),
          ZlibCompressor.CompressionHeader.GZIP_FORMAT,
          64 * 1024);
    }
  }

  static final class GzipZlibDecompressor extends ZlibDecompressor {
    public GzipZlibDecompressor() {
      super(ZlibDecompressor.CompressionHeader.AUTODETECT_GZIP_ZLIB, 64 * 1024);
    }
  }

}
