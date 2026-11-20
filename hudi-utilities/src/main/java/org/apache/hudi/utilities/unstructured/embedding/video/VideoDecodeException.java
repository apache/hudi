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

package org.apache.hudi.utilities.unstructured.embedding.video;

/**
 * A video whose bytes cannot be turned into frames (no video stream, a container or codec FFmpeg cannot read, a seek that does
 * not land on the chosen keyframe). A property of the data, the same on every attempt: the video's row ends PERMANENT.
 */
public class VideoDecodeException extends RuntimeException {

  private static final long serialVersionUID = 1L;

  public VideoDecodeException(String message) {
    super(message);
  }

  public VideoDecodeException(String message, Throwable cause) {
    super(message, cause);
  }
}
