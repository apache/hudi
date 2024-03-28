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

package org.apache.hudi

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import org.apache.avro.generic.GenericRecord
import org.apache.hadoop.test.LambdaTestUtils.intercept
import org.scalatest.{FunSpec, Matchers}

class JsonAvroConverterSpec extends FunSpec with Matchers {
  val mapper = new ObjectMapper()

  describe("Fetch the correct value") {
    it("should work well with record of primitive") {
      // boolean, int, long, double, string
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"boolean\"}," +
          "{\"name\": \"f2\", \"type\": \"int\"}," +
          "{\"name\": \"f3\", \"type\": \"long\"}," +
          "{\"name\": \"f4\", \"type\": \"double\"}," +
          "{\"name\": \"f5\", \"type\": \"string\"}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)

      val jsonStr: String =
        "{\"f1\": true, \"f2\": 4, \"f3\": 92233720368547758, \"f4\": 8.9, \"f5\": \"msg\"}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)
      
      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("f1") == true)
      assert(record.get("f2") == 4)
      assert(record.get("f3") == 92233720368547758L)
      assert(record.get("f4") == 8.9)
      assert(record.get("f5") == "msg")
    }
    
    it("should work well with record of primitive and array") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"boolean\"}," +
          "{\"name\": \"f2\", \"type\": \"int\"}," +
          "{\"name\": \"f3\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"string\"}]}," +
          "{\"name\": \"f4\", \"type\": [\"null\", {\"type\": \"array\", \"items\": \"int\"}]}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)

      val jsonStr: String =
        "{\"f1\": true, \"f2\": 4, \"f3\": [\"v1\",\"v2\",\"v3\"], \"f4\": [1, 2, 3]}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)
      
      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("f1") == true)
      assert(record.get("f2") == 4)
      assert(record.get("f3").toString == "[v1, v2, v3]")
      assert(record.get("f4").toString == "[1, 2, 3]")
    }
    
    it("should work well with array of record and primitive") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"boolean\"}," +
          "{\"name\": \"f2\", \"type\": \"int\"}," +
          "{\"name\": \"f3\", \"type\": " +
            "[\"null\", {" +
              "\"type\": \"array\", \"items\": {" +
                "\"type\": \"record\", " +
                "\"name\": \"inner_record\", " +
                "\"fields\": [ {" +
                  "\"name\": \"f31\", " +
                  "\"type\": \"int\"" +
                "}, {" +
                  "\"name\": \"f32\", " +
                  "\"type\": " + "\"int\"}]}}]}]}";
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)

      val jsonStr: String =
        "{\"f1\": true, \"f2\": 4, \"f3\": [{\"f31\": 1, \"f32\": 4},{\"f31\": 2, \"f32\": 5}]}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)

      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("f1") == true)
      assert(record.get("f2") == 4)
      assert(record.get("f3").toString == "[{\"f31\": 1, \"f32\": 4}, {\"f31\": 2, \"f32\": 5}]")
    }
    
    it{"should work well with null"} {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"boolean\"}," +
          "{\"name\": \"f2\", \"type\": \"int\"}," +
          "{\"name\": \"f3\", \"type\": \"long\"}," +
          "{\"name\": \"f4\", \"type\": \"double\"}," +
          "{\"name\": \"f5\", \"type\": \"string\"}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)

      val jsonStr: String =
        "{\"f1\": null, \"f2\": null, \"f3\": null, \"f4\": null, \"f5\": null}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)

      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("f1") == null)
      assert(record.get("f2") == null)
      assert(record.get("f3") == null)
      assert(record.get("f4") == null)
      assert(record.get("f5") == null)
    }
  }
  
  describe("Invalid inputs") {
    it("mandatory but missing") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"ts\", \"type\": \"double\", \"default\": 0}," +
          "{\"name\": \"host\", \"type\": \"string\", \"default\": \"\"}," +
          "{\"name\": \"level\", \"type\": \"int\"}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)
      
      val jsonStr: String =
        "{\"ts\": \"0.8\", \"host\": \"host1\"}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)

      val err = intercept[InvalidJsonException] { JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)}
      assert(err.getClass == classOf[InvalidJsonException])
      assert(err.getMessage == "Record mandatory field level missing")
    }
    
    it("optional and missing") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"ts\", \"type\": \"double\", \"default\": 0}," +
          "{\"name\": \"host\", \"type\": \"string\", \"default\": \"\"}," +
          "{\"name\": \"level\", \"type\": [\"null\", \"int\"]}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)
      
      val jsonStr: String =
        "{\"ts\": \"0.8\", \"host\": \"host1\"}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)

      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("ts") == 0.8)
      assert(record.get("host") == "host1")
      assert(record.get("level") == null)
    }
    
    it("optional and not missing") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"ts\", \"type\": \"double\", \"default\": 0}," +
          "{\"name\": \"host\", \"type\": \"string\", \"default\": \"\"}," +
          "{\"name\": \"level\", \"type\": [\"null\", \"int\"]}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)

      val jsonStr: String = "{\"ts\": 0.8, \"host\": \"host1\", \"level\": 1}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)

      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("ts") == 0.8)
      assert(record.get("host") == "host1")
      assert(record.get("level") == 1)
    }
    
    it("array and missing") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"ts\", \"type\": \"double\", \"default\": 0}," +
          "{\"name\": \"host\", \"type\": \"string\", \"default\": \"\"}," +
          "{\"name\": \"level\", \"type\": [\"null\", \"int\"]}," +
          "{\"name\": \"ids\", \"type\" : [\"null\", {\"type\": \"array\", \"items\": \"int\"}]}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)
      
      val jsonStr: String = "{\"ts\": 0.8, \"host\": \"host1\", \"level\": 1}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)


      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("ts") == 0.8)
      assert(record.get("host") == "host1")
      assert(record.get("level") == 1)
      assert(record.get("ids") == null)
    }
    
    it("array and not missing") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"ts\", \"type\": \"double\", \"default\": 0}," +
          "{\"name\": \"host\", \"type\": \"string\", \"default\": \"\"}," +
          "{\"name\": \"level\", \"type\": [\"null\", \"int\"]}," +
          "{\"name\": \"ids\", \"type\" : [\"null\", {\"type\": \"array\", \"items\": \"int\"}]}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)
      
      val jsonStr: String = "{\"ts\": 0.8, \"host\": \"host1\", \"level\": 1, \"ids\": [1,2,3]}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)

      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("ts") == 0.8)
      assert(record.get("host") == "host1")
      assert(record.get("level") == 1)
      assert(record.get("ids").toString == "[1, 2, 3]")
    }
  }

  describe("Type conversion") {
    it("Type can be converted") {
      val schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"int\", \"default\": 0}," +
          "{\"name\": \"f2\", \"type\": \"long\", \"default\": 0}," +
          "{\"name\": \"f3\", \"type\": \"double\", \"default\": 0}," +
          "{\"name\": \"f4\", \"type\": \"boolean\", \"default\": false}," +
          "{\"name\": \"f5\", \"type\": \"string\", \"default\": \"\"}]}"
      val schemaRoot: JsonNode = mapper.readTree(schemaStr)
      
      val jsonStr: String = "{\"f1\": \" 4\", " +
        "\"f2\": \" 123 \", " +
        "\"f3\": \"2.5d\", " +
        "\"f4\": \"true\", " +
        "\"f5\": \"v5\"}"
      val jsonRoot: JsonNode = mapper.readTree(jsonStr)

      val record: GenericRecord = JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)
      assert(record.get("f1") == 4)
      assert(record.get("f2") == 123)
      assert(record.get("f3") == 2.5)
      assert(record.get("f4") == true)
      assert(record.get("f5") == "v5")
    }

    it("Type can't be converted") {
      // int
      var schemaStr: String =
        "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"int\", \"default\": 0}]}"
      var schemaRoot: JsonNode = mapper.readTree(schemaStr)

      var jsonStr: String = "{\"f1\": \"4d\"}"
      var jsonRoot: JsonNode = mapper.readTree(jsonStr)

      var err = intercept[InvalidJsonException] { JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)}
      assert(err.getClass == classOf[InvalidJsonException])
      assert(err.getMessage == "Record value \"4d\" is not int. Field \"f1\"")

      // long
      schemaStr = "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"long\", \"default\": 0}]}"
      schemaRoot = mapper.readTree(schemaStr)

      jsonStr = "{\"f1\": \"4d\"}"
      jsonRoot = mapper.readTree(jsonStr)

      err = intercept[InvalidJsonException] { JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)}
      assert(err.getClass == classOf[InvalidJsonException])
      assert(err.getMessage == "Record value \"4d\" is not long. Field \"f1\"")
      
      // double
      schemaStr = "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"double\", \"default\": 0}]}"
      schemaRoot = mapper.readTree(schemaStr)

      jsonStr = "{\"f1\": \"t4.5d\"}"
      jsonRoot = mapper.readTree(jsonStr)

      err = intercept[InvalidJsonException] { JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)}
      assert(err.getClass == classOf[InvalidJsonException])
      assert(err.getMessage == "Record value \"t4.5d\" is not double. Field \"f1\"")
      
      // boolean
      schemaStr = "{\"type\": \"record\", " +
          "\"name\": \"sortsol_record\", " +
          "\"note\": \"note\", " +
          "\"fields\": [" +
          "{\"name\": \"f1\", \"type\": \"boolean\", \"default\": false}]}"
      schemaRoot = mapper.readTree(schemaStr)

      jsonStr = "{\"f1\": \"T\"}"
      jsonRoot = mapper.readTree(jsonStr)

      err = intercept[InvalidJsonException] { JsonAvroConverter.visitRecord(schemaRoot, jsonRoot)}
      assert(err.getClass == classOf[InvalidJsonException])
      assert(err.getMessage == "Record value \"T\" is not boolean. Field \"f1\"")
    }
  }
}
