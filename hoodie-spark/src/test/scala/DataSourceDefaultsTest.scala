/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

import com.uber.hoodie.{DataSourceWriteOptions, OverwriteWithLatestAvroPayload, SimpleKeyGenerator}
import com.uber.hoodie.common.util.SchemaTestUtil
import com.uber.hoodie.exception.HoodieException
import org.apache.avro.generic.GenericRecord
import org.apache.commons.configuration.PropertiesConfiguration
import org.junit.Assert._
import org.junit.{Before, Test}
import org.scalatest.junit.AssertionsForJUnit

/**
  * Tests on the default key generator, payload classes.
  */
class DataSourceDefaultsTest extends AssertionsForJUnit {

  val schema = SchemaTestUtil.getComplexEvolvedSchema
  var baseRecord : GenericRecord = null

  @Before def initialize(): Unit = {
    baseRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 1, "001", "f1")
  }


  private def getKeyConfig(recordKeyFieldName: String, paritionPathField: String): PropertiesConfiguration  = {
    val props = new PropertiesConfiguration()
    props.addProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, recordKeyFieldName)
    props.addProperty(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, paritionPathField)
    props
  }
  @Test def testSimpleKeyGenerator() = {
    // top level, valid fields
    val hk1 = new SimpleKeyGenerator(getKeyConfig("field1", "name")).getKey(baseRecord)
    assertEquals("field1", hk1.getRecordKey)
    assertEquals("name1", hk1.getPartitionPath)

    // recordKey field not specified
    try {
      val props = new PropertiesConfiguration()
      props.addProperty(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "field1")
      new SimpleKeyGenerator(props).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieException => {
        // do nothing
      }
    };

    // partitionPath field is null
    try {
      new SimpleKeyGenerator(getKeyConfig("field1", null)).getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieException => {
        // do nothing
      }
    };

    // nested field as record key and partition path
    val hk2 = new SimpleKeyGenerator(getKeyConfig("testNestedRecord.userId", "testNestedRecord.isAdmin"))
      .getKey(baseRecord)
    assertEquals("UserId1@001", hk2.getRecordKey)
    assertEquals("false", hk2.getPartitionPath)

    // Nested record key not found
    try {
      new SimpleKeyGenerator(getKeyConfig("testNestedRecord.NotThere", "testNestedRecord.isAdmin"))
        .getKey(baseRecord)
      fail("Should have errored out")
    } catch {
      case e: HoodieException => {
        // do nothing
      }
    };
  }

  @Test def testOverwriteWithLatestAvroPayload() = {
    val overWritePayload1 = new OverwriteWithLatestAvroPayload(baseRecord, 1)
    val laterRecord = SchemaTestUtil
      .generateAvroRecordFromJson(schema, 2, "001", "f1")
    val overWritePayload2 = new OverwriteWithLatestAvroPayload(laterRecord, 2)

    // it will provide the record with greatest combine value
    val combinedPayload12 = overWritePayload1.preCombine(overWritePayload2)
    val combinedGR12 = combinedPayload12.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field2", combinedGR12.get("field1"))

    // and it will be deterministic, to order of processing.
    val combinedPayload21 = overWritePayload2.preCombine(overWritePayload1)
    val combinedGR21 = combinedPayload21.getInsertValue(schema).get().asInstanceOf[GenericRecord]
    assertEquals("field2", combinedGR21.get("field1"))
  }
}
