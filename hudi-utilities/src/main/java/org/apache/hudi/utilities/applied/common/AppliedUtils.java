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

package org.apache.hudi.utilities.applied.common;

import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;

public class AppliedUtils {

  public static byte[] getS3ObjectAsBytes(String region, String bucket, String key) {
    try (S3Client s3Client = S3Client.builder().region(Region.of(region)).build()) {
      return getS3ObjectAsBytes(s3Client, bucket, key);
    }
  }

  public static byte[] getS3ObjectAsBytes(S3Client s3Client, String bucket, String key) {
    GetObjectRequest objectRequest = GetObjectRequest
        .builder()
        .key(key)
        .bucket(bucket)
        .build();
    ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObjectAsBytes(objectRequest);
    return objectBytes.asByteArray();
  }

  public static String camelToSnake(String str) {
    // Copied from https://www.geeksforgeeks.org/convert-camel-case-string-to-snake-case-in-java/
    // Empty String
    StringBuilder result = new StringBuilder();

    // Append first character(in lower case)
    // to result string
    char c = str.charAt(0);
    result.append(Character.toLowerCase(c));

    // Traverse the string from
    // ist index to last index
    for (int i = 1; i < str.length(); i++) {
      char ch = str.charAt(i);
      // Check if the character is upper case
      // then append '_' and such character
      // (in lower case) to result string
      if (Character.isUpperCase(ch)) {
        result.append('_');
        result.append(Character.toLowerCase(ch));
      } else {
        result.append(ch);
      }
    }
    // return the result
    return result.toString();
  }
}
