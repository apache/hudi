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

package org.apache.hudi

import org.apache.hudi.client.transaction.lock.FileSystemBasedLockProvider
import org.apache.hudi.common.config.{HoodieCommonConfig, TypedProperties}
import org.apache.hudi.common.table.{HoodieTableConfig, HoodieTableMetaClient}
import org.apache.hudi.config.HoodieLockConfig

import org.apache.spark.sql.{SparkSession, SQLContext}
import org.junit.jupiter.api.Assertions.{assertEquals, assertThrows, assertTrue}
import org.junit.jupiter.api.Test
import org.mockito.Mockito.{mock, when}

class TestHoodieCLIUtils {

  @Test
  def testExtractOptionsBasic(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions("k1=v1,k2=v2")
    assertEquals(2, parsed.size)
    assertEquals("v1", parsed("k1"))
    assertEquals("v2", parsed("k2"))
  }

  @Test
  def testExtractOptionsTrimsWhitespace(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions(" k1 = v1 ,  k2= v 2 ")
    assertEquals("v1", parsed("k1"))
    // internal whitespace inside value is preserved, only edges are trimmed
    assertEquals("v 2", parsed("k2"))
  }

  @Test
  def testExtractOptionsIgnoresEmptyTokens(): Unit = {
    // trailing comma, consecutive commas, leading comma — all silently ignored
    val parsed = HoodieCLIUtils.extractOptions(",k1=v1,, ,k2=v2,")
    assertEquals(2, parsed.size)
    assertEquals("v1", parsed("k1"))
    assertEquals("v2", parsed("k2"))
  }

  @Test
  def testExtractOptionsValueContainsEquals(): Unit = {
    // only the first `=` should be treated as a delimiter
    val parsed = HoodieCLIUtils.extractOptions("k=a=b=c")
    assertEquals(1, parsed.size)
    assertEquals("a=b=c", parsed("k"))
  }

  @Test
  def testExtractOptionsAllowsEmptyValue(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions("k=")
    assertEquals(1, parsed.size)
    assertEquals("", parsed("k"))
  }

  @Test
  def testExtractOptionsDuplicateKeyLastWins(): Unit = {
    val parsed = HoodieCLIUtils.extractOptions("k=v1,k=v2,k=v3")
    assertEquals(1, parsed.size)
    assertEquals("v3", parsed("k"))
  }

  @Test
  def testExtractOptionsNullAndEmpty(): Unit = {
    assertTrue(HoodieCLIUtils.extractOptions(null).isEmpty)
    assertTrue(HoodieCLIUtils.extractOptions("").isEmpty)
    assertTrue(HoodieCLIUtils.extractOptions("   ").isEmpty)
    assertTrue(HoodieCLIUtils.extractOptions(",,, ").isEmpty)
  }

  @Test
  def testExtractOptionsThrowsOnMissingDelimiter(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => HoodieCLIUtils.extractOptions("k1=v1,invalid"))
    assertTrue(ex.getMessage.contains("invalid"))
  }

  @Test
  def testExtractOptionsThrowsOnEmptyKey(): Unit = {
    val ex = assertThrows(
      classOf[IllegalArgumentException],
      () => HoodieCLIUtils.extractOptions("=v"))
    assertTrue(ex.getMessage.contains("key=value") || ex.getMessage.contains("Option key"))
  }

  @Test
  def testExtractOptionsThrowsOnWhitespaceKey(): Unit = {
    assertThrows(
      classOf[IllegalArgumentException],
      () => HoodieCLIUtils.extractOptions("   =v"))
  }

  @Test
  def testGetLockOptionsSupportedSchemeReturnsFsLockConfig(): Unit = {
    val tablePath = "/tmp/hudi/some_table"
    // A null scheme is treated as supported; the FS lock provider must be auto-configured.
    val opts = HoodieCLIUtils.getLockOptions(tablePath, null, Map.empty)
    assertEquals(classOf[FileSystemBasedLockProvider].getName,
      opts(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key))
    assertEquals(s"$tablePath/${HoodieTableMetaClient.AUXILIARYFOLDER_NAME}",
      opts(HoodieLockConfig.FILESYSTEM_LOCK_PATH.key))
  }

  @Test
  def testGetLockOptionsUnsupportedSchemeReturnsEmpty(): Unit = {
    // s3 is a known scheme without atomic-creation support, so no FS lock can be configured.
    assertTrue(HoodieCLIUtils.getLockOptions("s3://bucket/table", "s3", Map.empty).isEmpty)
  }

  @Test
  def testGetLockOptionsCustomAtomicSupportEnablesScheme(): Unit = {
    // Opting s3 into hoodie.fs.atomic_creation.support makes the FS lock provider eligible again.
    val params = Map(HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key -> " hdfs, s3 ")
    val opts = HoodieCLIUtils.getLockOptions("s3://bucket/table", "s3", params)
    assertEquals(classOf[FileSystemBasedLockProvider].getName,
      opts(HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key))
  }

  @Test
  def testWriteParametersPreserveLockConfigPrecedence(): Unit = {
    val sparkSession = mock(classOf[SparkSession])
    val sqlContext = mock(classOf[SQLContext])
    val metaClient = mock(classOf[HoodieTableMetaClient])
    val tableConfig = mock(classOf[HoodieTableConfig])
    val tableProps = new TypedProperties()
    val providerKey = HoodieLockConfig.LOCK_PROVIDER_CLASS_NAME.key
    val atomicSupportKey = HoodieCommonConfig.HOODIE_FS_ATOMIC_CREATION_SUPPORT.key
    tableProps.setProperty(providerKey, "table.provider")
    tableProps.setProperty(atomicSupportKey, "hdfs")
    when(sparkSession.sqlContext).thenReturn(sqlContext)
    when(metaClient.getTableConfig).thenReturn(tableConfig)
    when(tableConfig.getProps).thenReturn(tableProps)
    when(sqlContext.getAllConfs).thenReturn(Map.empty[String, String])

    val fromTable = HoodieCLIUtils.getWriteParameters(sparkSession, metaClient, Map.empty, None)
    assertEquals("table.provider", fromTable(providerKey))
    assertEquals("hdfs", fromTable(atomicSupportKey))

    when(sqlContext.getAllConfs).thenReturn(Map(providerKey -> "session.provider", atomicSupportKey -> "s3"))
    val fromSession = HoodieCLIUtils.getWriteParameters(sparkSession, metaClient, Map.empty, None)
    assertEquals("session.provider", fromSession(providerKey))
    assertEquals("s3", fromSession(atomicSupportKey))

    val fromOptions = HoodieCLIUtils.getWriteParameters(sparkSession, metaClient,
      Map(providerKey -> "options.provider", atomicSupportKey -> "file"), None)
    assertEquals("options.provider", fromOptions(providerKey))
    assertEquals("file", fromOptions(atomicSupportKey))
    assertEquals(fromOptions,
      HoodieCLIUtils.getWriteParameters(sparkSession, metaClient, fromOptions, None))

    tableProps.clear()
    when(sqlContext.getAllConfs).thenReturn(Map.empty[String, String])
    val withoutLock = HoodieCLIUtils.getWriteParameters(sparkSession, metaClient, Map.empty, None)
    assertTrue(!withoutLock.contains(providerKey))
  }

}
