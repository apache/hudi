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

package org.apache.hudi.utilities.applied.common;

import java.io.Serializable;

public class S3Info implements Serializable {
  private static final long serialVersionUID = 1L;

  private BucketInfo bucket;
  private ObjectInfo object;

  public S3Info() {
    this.bucket = new BucketInfo();
    this.object = new ObjectInfo();
  }

  public S3Info(BucketInfo bucket, ObjectInfo object) {
    this.bucket = bucket;
    this.object = object;
  }

  public BucketInfo getBucket() {
    return bucket;
  }

  public void setBucket(BucketInfo bucket) {
    this.bucket = bucket;
  }

  public ObjectInfo getObject() {
    return object;
  }

  public void setObject(ObjectInfo object) {
    this.object = object;
  }
}
