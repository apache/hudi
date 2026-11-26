/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.gcp.bundle;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.storage.StorageOptions;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class ITGcpBundleCompatibility {

  private enum ClasspathOrder {
    HOST_FIRST,
    BUNDLE_FIRST
  }

  @Test
  void testHostDependenciesBeforeBundle() throws Exception {
    runSmokeTest(ClasspathOrder.HOST_FIRST);
  }

  @Test
  void testBundleBeforeHostDependencies() throws Exception {
    runSmokeTest(ClasspathOrder.BUNDLE_FIRST);
  }

  @Test
  void testOrcUtilsDoesNotLinkToShadedProtobuf() throws Exception {
    try (JarFile bundle = new JarFile(System.getProperty("gcp.bundle.jar"))) {
      JarEntry entry = bundle.getJarEntry("org/apache/hudi/common/util/OrcUtils.class");
      assertNotNull(entry);
      try (InputStream input = bundle.getInputStream(entry);
           ByteArrayOutputStream output = new ByteArrayOutputStream()) {
        byte[] buffer = new byte[4096];
        int count;
        while ((count = input.read(buffer)) != -1) {
          output.write(buffer, 0, count);
        }
        String bytecode = new String(output.toByteArray(), StandardCharsets.ISO_8859_1);
        assertFalse(bytecode.contains("org/apache/hudi/gcp/shaded/com/google/protobuf/"),
            "OrcUtils must not link to protobuf types relocated by the GCP bundle");
      }
    }
  }

  private static void runSmokeTest(ClasspathOrder order) throws Exception {
    String bundleJar = System.getProperty("gcp.bundle.jar");
    String testClasses = System.getProperty("gcp.test.classes");
    String hostProtobuf = System.getProperty("host.protobuf.jar");
    String hostGuava = System.getProperty("host.guava.jar");
    String hostJacksonCore = System.getProperty("host.jackson.core.jar");

    List<String> orderedClasspath = new ArrayList<>();
    orderedClasspath.add(testClasses);
    if (order == ClasspathOrder.HOST_FIRST) {
      orderedClasspath.add(hostProtobuf);
      orderedClasspath.add(hostGuava);
    }
    orderedClasspath.add(bundleJar);
    if (order == ClasspathOrder.BUNDLE_FIRST) {
      orderedClasspath.add(hostProtobuf);
      orderedClasspath.add(hostGuava);
    }
    orderedClasspath.add(hostJacksonCore);

    Process process = new ProcessBuilder(
        new File(System.getProperty("java.home"), "bin/java").getAbsolutePath(),
        "-cp",
        String.join(File.pathSeparator, orderedClasspath),
        ITGcpBundleCompatibility.class.getName(),
        "smoke")
        .inheritIO()
        .start();
    assertEquals(0, process.waitFor(), "GCP bundle compatibility smoke test failed");
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 1 || !"smoke".equals(args[0])) {
      throw new IllegalArgumentException("Expected smoke mode");
    }

    verifyShadedProtobuf();
    verifyShadedGuava();
    verifyHostGuava();
    verifyStorageClient();
    verifyBigQueryClient();
    verifyThreeTenExtra();
  }

  private static void verifyShadedProtobuf() throws Exception {
    Class<?> timestampClass = Class.forName(
        "org.apache.hudi.gcp.shaded.com.google.protobuf.Timestamp");
    Object timestampBuilder = timestampClass.getMethod("newBuilder").invoke(null);
    Method setSeconds = timestampBuilder.getClass().getMethod("setSeconds", long.class);
    Object timestamp = timestampBuilder.getClass().getMethod("build")
        .invoke(setSeconds.invoke(timestampBuilder, 1L));
    byte[] timestampBytes = (byte[]) timestampClass.getMethod("toByteArray").invoke(timestamp);
    if (timestampBytes.length == 0) {
      throw new AssertionError("Protobuf serialization returned no bytes");
    }
  }

  private static void verifyShadedGuava() throws Exception {
    Class<?> domainNameClass = Class.forName(
        "org.apache.hudi.com.google.common.net.InternetDomainName");
    Object domainName = domainNameClass.getMethod("from", String.class).invoke(null, "example.com");
    Object publicSuffix = domainNameClass.getMethod("publicSuffix").invoke(domainName);
    if (!"com".equals(publicSuffix.toString())) {
      throw new AssertionError("Unexpected public suffix: " + publicSuffix);
    }
  }

  private static void verifyHostGuava() throws Exception {
    Class<?> hostDomainNameClass = Class.forName("com.google.common.net.InternetDomainName");
    Object hostDomainName = hostDomainNameClass.getMethod("from", String.class)
        .invoke(null, "example.com");
    Object hostPublicSuffix = hostDomainNameClass.getMethod("publicSuffix")
        .invoke(hostDomainName);
    if (!"com".equals(hostPublicSuffix.toString())) {
      throw new AssertionError("Unexpected host public suffix: " + hostPublicSuffix);
    }
  }

  private static void verifyStorageClient() {
    StorageOptions options = StorageOptions.newBuilder()
        .setProjectId("review")
        .setCredentials(NoCredentials.getInstance())
        .setRetrySettings(StorageOptions.getDefaultRetrySettings().toBuilder()
            .setMaxAttempts(1)
            .build())
        .build();
    options.getService();
  }

  private static void verifyBigQueryClient() {
    BigQueryOptions.newBuilder()
        .setProjectId("review")
        .setCredentials(NoCredentials.getInstance())
        .build()
        .getService();
  }

  private static void verifyThreeTenExtra() throws Exception {
    Class<?> periodDurationClass = Class.forName("org.threeten.extra.PeriodDuration");
    Object periodDuration = periodDurationClass.getMethod("parse", CharSequence.class)
        .invoke(null, "P1D");
    if (!"P1D".equals(periodDuration.toString())) {
      throw new AssertionError("Unexpected period duration: " + periodDuration);
    }
  }
}
