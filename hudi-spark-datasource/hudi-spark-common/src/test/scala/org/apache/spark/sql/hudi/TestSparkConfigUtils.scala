package org.apache.spark.sql.hudi

import org.apache.hudi.common.config.ConfigProperty
import org.apache.hudi.util.SparkConfigUtils
import org.junit.jupiter.api.Assertions.{assertEquals, assertFalse, assertTrue}
import org.junit.jupiter.api.Test

class TestSparkConfigUtils {

  val TEST_BOOLEAN_CONFIG_PROPERTY: ConfigProperty[String] = ConfigProperty
    .key("hoodie.test.boolean.config")
    .defaultValue("false")
    .withAlternatives("alternate.hoodie.test.boolean.config")
    .markAdvanced
    .withDocumentation("Testing boolean config.")

  @Test
  def testWithAltKeys(): Unit = {
    var map : Map[String, String] = Map.empty
    map += ("alternate.hoodie.test.boolean.config" -> "true")
    // Ensure alternate key gets picked up
    assertEquals("true", SparkConfigUtils.getStringWithAltKeys(map, TEST_BOOLEAN_CONFIG_PROPERTY))
    assertTrue(SparkConfigUtils.containsConfigProperty(map, TEST_BOOLEAN_CONFIG_PROPERTY))

    map += ("alternate.hoodie.test.boolean.config" -> "false")
    map += ("hoodie.test.boolean.config" -> "true")
    // Ensure actual key gets picked up
    assertEquals("true", SparkConfigUtils.getStringWithAltKeys(map, TEST_BOOLEAN_CONFIG_PROPERTY))
    assertTrue(SparkConfigUtils.containsConfigProperty(map, TEST_BOOLEAN_CONFIG_PROPERTY))

    map = Map.empty
    assertEquals("false", SparkConfigUtils.getStringWithAltKeys(map, TEST_BOOLEAN_CONFIG_PROPERTY))
    assertFalse(SparkConfigUtils.containsConfigProperty(map, TEST_BOOLEAN_CONFIG_PROPERTY))
  }
}
