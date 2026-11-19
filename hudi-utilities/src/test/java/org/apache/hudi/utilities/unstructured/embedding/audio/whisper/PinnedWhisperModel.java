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

package org.apache.hudi.utilities.unstructured.embedding.audio.whisper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The whisper model the CI test runs on: ggml {@value #NAME} (MIT, whisper.cpp), pinned by repository revision, size and
 * SHA-256, fetched once into a cache directory ({@value #CACHE_DIR_PROPERTY}, default {@code target/whisper-models}) and
 * verified on every use. A download that fails or a file whose size or digest differs fails the caller: it is never skipped.
 */
final class PinnedWhisperModel {

  static final String NAME = "tiny.en";
  /** huggingface ggerganov/whisper.cpp at the revision the gated tests' fetch script pins (base.en a03779c8... there). */
  static final String URL = "https://huggingface.co/ggerganov/whisper.cpp/resolve/5359861c739e955e79d9a303bcbc70fb988958b1/ggml-"
      + NAME + ".bin";
  static final long SIZE = 77_704_715L;
  static final String SHA256 = "921e4cf8686fdd993dcd081a5da5b6c365bfde1162e72b08d75ac75289920b1f";
  static final String CACHE_DIR_PROPERTY = "hudi.test.whisper.ci.model.dir";

  private static final int TIMEOUT_MILLIS = 60_000;
  private static final int ATTEMPTS = 3;

  private PinnedWhisperModel() {
  }

  /** The verified model file, downloaded first when the cache does not hold it. */
  static synchronized Path get() throws IOException {
    Path dir = Paths.get(System.getProperty(CACHE_DIR_PROPERTY, Paths.get("target", "whisper-models").toString()))
        .toAbsolutePath();
    Path model = dir.resolve("ggml-" + NAME + ".bin");
    if (Files.isRegularFile(model) && Files.size(model) == SIZE && SHA256.equals(sha256(model))) {
      return model;
    }
    Files.createDirectories(dir);
    Path part = dir.resolve("ggml-" + NAME + ".bin.part");
    IOException last = null;
    for (int attempt = 1; attempt <= ATTEMPTS; attempt++) {
      try {
        download(part);
        verify(part);
        return Files.move(part, model, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
      } catch (IOException e) {
        last = e;
        Files.deleteIfExists(part);
        backOff(attempt);
      }
    }
    throw new IOException("whisper model " + NAME + " could not be fetched from " + URL + " in " + ATTEMPTS + " attempts", last);
  }

  /** Waits 5 s, then 10 s, before the next attempt: a transient network or mirror failure gets time to clear. */
  private static void backOff(int attempt) throws IOException {
    if (attempt < ATTEMPTS) {
      try {
        // by design: a fixed retry backoff (not a wait for a condition), so a transient mirror failure gets time to clear
        Thread.sleep(5_000L * attempt);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("interrupted while fetching whisper model " + NAME, e);
      }
    }
  }

  /** Throws unless {@code file} has the pinned size and SHA-256. */
  static void verify(Path file) throws IOException {
    long size = Files.size(file);
    String digest = sha256(file);
    if (size != SIZE || !SHA256.equals(digest)) {
      throw new IOException("whisper model " + file + " is " + size + " bytes with sha-256 " + digest + "; pinned: " + SIZE
          + " bytes, " + SHA256);
    }
  }

  private static void download(Path target) throws IOException {
    HttpURLConnection connection = (HttpURLConnection) new URL(URL).openConnection();
    connection.setConnectTimeout(TIMEOUT_MILLIS);
    connection.setReadTimeout(TIMEOUT_MILLIS);
    connection.setInstanceFollowRedirects(true);
    try {
      int status = connection.getResponseCode();
      if (status != HttpURLConnection.HTTP_OK) {
        throw new IOException("GET " + URL + " answered " + status);
      }
      try (InputStream in = connection.getInputStream(); OutputStream out = Files.newOutputStream(target)) {
        byte[] buffer = new byte[1 << 16];
        for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
          out.write(buffer, 0, n);
        }
      }
    } finally {
      connection.disconnect();
    }
  }

  static String sha256(Path file) throws IOException {
    MessageDigest sha;
    try {
      sha = MessageDigest.getInstance("SHA-256");
    } catch (NoSuchAlgorithmException e) {
      throw new IllegalStateException(e);
    }
    try (InputStream in = Files.newInputStream(file)) {
      byte[] buffer = new byte[1 << 16];
      for (int n = in.read(buffer); n >= 0; n = in.read(buffer)) {
        sha.update(buffer, 0, n);
      }
    }
    StringBuilder hex = new StringBuilder();
    for (byte b : sha.digest()) {
      hex.append(String.format("%02x", b));
    }
    return hex.toString();
  }
}
