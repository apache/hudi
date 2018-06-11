/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.util.collection.converter;

/**
 * A converter interface to getBytes or deserialize a payload. This is used in {@link
 * com.uber.hoodie.common.util.collection.ExternalSpillableMap} to spillToDisk
 */
public interface Converter<T> {

  /**
   * This method is used to convert a payload to bytes
   */
  byte[] getBytes(T t);

  /**
   * This method is used to convert the serialized payload (in bytes) to the actual payload instance
   */
  T getData(byte[] bytes);
}
