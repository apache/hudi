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

package org.apache.hudi.utilities.unstructured.storage;

import org.apache.hudi.storage.StorageConfiguration;
import org.apache.hudi.storage.hadoop.HadoopStorageConfiguration;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.net.URI;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** The served-scheme and missing-authority rules of a storage reference. */
class TestStorageReferenceRules {

  /** A supported scheme with no connector cannot serve a row in this job. */
  @ParameterizedTest
  @ValueSource(strings = {"oss", "Oss", "cosn", "missingconnector"})
  void unavailableSchemesAreNotServed(String scheme) {
    Configuration conf = new Configuration(false);
    assertFalse(StorageReferenceRules.isServedScheme(scheme, conf));
    assertFalse(StorageReferenceRules.isServedScheme(scheme, new HadoopStorageConfiguration(conf)));
  }

  @Test
  void configuredImplementationServesAScheme() {
    Configuration conf = new Configuration(false);
    assertTrue(StorageReferenceRules.isServedScheme("file", conf));
    assertFalse(StorageReferenceRules.isServedScheme("rulesfs", conf));
    conf.set("fs.rulesfs.impl", LocalFileSystem.class.getName());
    assertTrue(StorageReferenceRules.isServedScheme("rulesfs", conf));
    assertTrue(StorageReferenceRules.isServedScheme("RulesFS", new HadoopStorageConfiguration(conf)));
  }

  @Test
  void foreignOrAbsentSchemeIsNotServed() {
    Configuration conf = new Configuration(false);
    assertFalse(StorageReferenceRules.isServedScheme("foo", conf));
    assertFalse(StorageReferenceRules.isServedScheme(null, conf));
    assertFalse(StorageReferenceRules.isServedScheme(null, unreadable()));
  }

  /** s3n is not an upstream Hudi scheme: it is served only when the job configures its implementation. */
  @Test
  void s3nIsServedOnlyWhenConfigured() {
    Configuration conf = new Configuration(false);
    assertFalse(StorageReferenceRules.isServedScheme("s3n", conf));
    conf.set("fs.s3n.impl", LocalFileSystem.class.getName());
    assertTrue(StorageReferenceRules.isServedScheme("s3n", conf));
  }

  /** An unreadable job configuration is not proof that a scheme is foreign, nor that an authority is missing. */
  @Test
  void unreadableConfigurationIsServedAndNeverMissingAnAuthority() {
    assertTrue(StorageReferenceRules.isServedScheme("foo", unreadable()));
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("hdfs:///docs/a.pdf"), unreadable()));
    // every read path of Configuration (get, get with a default, getTrimmed) goes through getProps on Hadoop 2.10 to 3.4
    Configuration unreadableConf = new Configuration(false) {
      @Override
      protected synchronized Properties getProps() {
        throw new IllegalStateException("job configuration unavailable");
      }
    };
    assertTrue(StorageReferenceRules.isServedScheme("foo", unreadableConf));
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("hdfs:///docs/a.pdf"), unreadableConf));
    assertTrue(StorageReferenceRules.isServedScheme("foo", (Configuration) null));
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("hdfs:///docs/a.pdf"), (Configuration) null));
  }

  @Test
  void missingAuthorityIsSuppliedOnlyByTheSameSchemeDefault() {
    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "hdfs://namenode/");
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("hdfs:///docs/a.pdf"), conf));
    assertTrue(StorageReferenceRules.isMissingRequiredAuthority(URI.create("s3a:///docs/a.pdf"), conf));
    conf.set("fs.defaultFS", "file:///");
    assertTrue(StorageReferenceRules.isMissingRequiredAuthority(URI.create("hdfs:///docs/a.pdf"), new HadoopStorageConfiguration(conf)));
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("hdfs://namenode/docs/a.pdf"), conf));
    // every bucket- or container-addressed scheme needs one, not only hdfs and s3a
    for (String scheme : new String[] {"s3", "gs", "wasb", "wasbs", "abfs", "abfss"}) {
      assertTrue(StorageReferenceRules.isMissingRequiredAuthority(URI.create(scheme + ":///docs/a.pdf"), conf), scheme);
      assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create(scheme + "://bucket/docs/a.pdf"), conf), scheme);
    }
    // a scheme that may legitimately go without one is not malformed for its absence
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("viewfs:///docs/a.pdf"), conf));
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("file:///docs/a.pdf"), conf));
    assertTrue(StorageReferenceRules.isMissingRequiredAuthority(URI.create("HDFS:///docs/a.pdf"), conf), "schemes compare in any case");
    conf.set("fs.defaultFS", "HDFS://namenode/");
    assertFalse(StorageReferenceRules.isMissingRequiredAuthority(URI.create("hdfs:///docs/a.pdf"), conf));
  }

  /** The invalid-authority and out-of-range-port rules both readers of a reference share. */
  @Test
  void invalidAuthorityAndOutOfRangePortAreTheReferencesOwn() {
    URI noHost = URI.create("s3a://bad_bucket/x");
    assertTrue(StorageReferenceRules.isInvalidAuthorityFailure(noHost, new IllegalArgumentException("bucket")));
    assertTrue(StorageReferenceRules.isInvalidAuthorityFailure(noHost, new NullPointerException("host")));
    assertFalse(StorageReferenceRules.isInvalidAuthorityFailure(noHost, new IllegalStateException("other")));
    assertFalse(StorageReferenceRules.isInvalidAuthorityFailure(URI.create("s3a://bucket/x"), new IllegalArgumentException("x")));
    Configuration conf = new Configuration(false);
    conf.set("fs.defaultFS", "file:///");
    URI badPort = URI.create("hdfs://localhost:65536/x");
    assertTrue(StorageReferenceRules.isMalformedConstruction(badPort,
        new RuntimeException(new IllegalArgumentException("port out of range")), conf), "argument failure in the chain");
    assertFalse(StorageReferenceRules.isMalformedConstruction(badPort, new RuntimeException("io"), conf), "no argument failure");
    assertFalse(StorageReferenceRules.isMalformedConstruction(URI.create("hdfs://localhost:8020/x"),
        new IllegalArgumentException("unknown host"), conf), "a host that does not resolve is not the reference's fault");
    assertTrue(StorageReferenceRules.isMalformedConstruction(URI.create("s3a:///x"), new RuntimeException("io"), conf));
    assertTrue(StorageReferenceRules.isMalformedConstruction(URI.create("s3a:///x"), new RuntimeException("io"),
        new HadoopStorageConfiguration(conf)));
  }

  private static StorageConfiguration<?> unreadable() {
    return new HadoopStorageConfiguration(new Configuration(false)) {
      @Override
      public Configuration unwrap() {
        throw new IllegalStateException("job configuration unavailable");
      }
    };
  }
}
