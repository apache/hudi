/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.utilities.unstructured.embedding.audio;

import org.apache.hudi.utilities.unstructured.embedding.source.BlobBytesSupplier;

/**
 * What an {@link AudioSegmentSource} is given about ONE video whose speech becomes speech rows:
 * data prep's video preprocessor builds it from the video's blob-table row.
 */
public interface AudioInput {

  /** The blob-table record key of the video; every row the audio pass writes carries it. */
  String sourcePathSha();

  /** The blob row's original path ({@code source_path}), for logging only; null when unknown (data prep does not pass it). */
  String sourcePath();

  /** The blob row's partition path (its {@code _hoodie_partition_path}); null for an unpartitioned table. */
  String sourcePartitionPath();

  /** The video's size in bytes from the blob row's {@code file_size}, or {@code -1} when unknown. */
  long sizeBytes();

  /** The lower-case file extension (e.g. {@code mp4}); selects nothing by itself, the demuxer probes the container. */
  String extension();

  /**
   * Opens the video's bytes (inline blob or out-of-line file), once per call. A source reads it as a stream; it never
   * copies the whole video into memory.
   */
  BlobBytesSupplier bytes();
}
