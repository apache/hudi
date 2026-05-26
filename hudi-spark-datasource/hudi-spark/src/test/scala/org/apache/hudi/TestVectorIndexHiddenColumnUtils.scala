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

import org.apache.hudi.common.index.vector.RaBitQEncoder
import org.apache.hudi.common.model.HoodieIndexDefinition
import org.apache.hudi.common.schema.HoodieSchema
import org.apache.hudi.metadata.HoodieIndexVersion
import org.apache.hudi.metadata.HoodieTableMetadataUtil.PARTITION_NAME_VECTOR_INDEX

import org.scalatest.{FunSuite, Matchers}

import java.util.{Collections, HashMap}

class TestVectorIndexHiddenColumnUtils extends FunSuite with Matchers {

  test("test vector hidden column names use logical index name") {
    val indexDefinition = HoodieIndexDefinition.newBuilder()
      .withIndexName("vector_index_demo_idx")
      .withIndexType(PARTITION_NAME_VECTOR_INDEX)
      .withSourceFields(Collections.singletonList("embedding"))
      .withIndexOptions(Collections.singletonMap("vector.dimension", "8"))
      .withVersion(HoodieIndexVersion.V1)
      .build()

    VectorIndexHiddenColumnUtils.getLogicalIndexName(indexDefinition) shouldBe "demo_idx"
    VectorIndexHiddenColumnUtils.getBinaryCodeColumnName(indexDefinition) shouldBe "_hudi_vec_demo_idx_binary_code"
    VectorIndexHiddenColumnUtils.getScalarColumnName(indexDefinition) shouldBe "_hudi_vec_demo_idx_scalar"
  }

  test("test rabitq encoding from spark array values") {
    val vectorSchema = HoodieSchema.createVector(8, HoodieSchema.Vector.VectorElementType.FLOAT)
    val encoder = new RaBitQEncoder(8, 42L, false)
    val encodedCode = VectorIndexHiddenColumnUtils.encodeBinaryCode(Seq(1.0f, -1.0f, 2.0f, -2.0f, 3.0f, -3.0f, 4.0f, -4.0f), vectorSchema, encoder)
    val encodedScalar = VectorIndexHiddenColumnUtils.encodeScalar(Seq(1.0f, -1.0f, 2.0f, -2.0f, 3.0f, -3.0f, 4.0f, -4.0f), vectorSchema, encoder)

    encodedCode.length shouldBe encoder.codeBytes()
    encodedScalar shouldBe RaBitQEncoder.norm(Array(1.0f, -1.0f, 2.0f, -2.0f, 3.0f, -3.0f, 4.0f, -4.0f))
  }

  test("test int8 vectors are converted to float inputs for rabitq") {
    val vectorSchema = HoodieSchema.createVector(4, HoodieSchema.Vector.VectorElementType.INT8)
    val encoder = new RaBitQEncoder(4, 7L, true)
    val encodedCode = VectorIndexHiddenColumnUtils.encodeBinaryCode(Seq[Byte](1, -1, 2, -2), vectorSchema, encoder)
    val encodedScalar = VectorIndexHiddenColumnUtils.encodeScalar(Seq[Byte](1, -1, 2, -2), vectorSchema, encoder)

    encodedCode.length shouldBe encoder.codeBytes()
    encodedScalar shouldBe 1.0f
  }
}
