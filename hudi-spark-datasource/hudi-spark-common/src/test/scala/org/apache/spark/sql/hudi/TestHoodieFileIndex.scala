package org.apache.spark.sql.hudi

import org.apache.hudi.HoodieFileIndex
import org.apache.hudi.common.table.HoodieTableConfig

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

class TestHoodieFileIndex {
  @Test
  def testDefaultDatabaseName(): Unit = {
    assertEquals(HoodieFileIndex.getDatabaseName(new HoodieTableConfig()), "default")
  }
}
