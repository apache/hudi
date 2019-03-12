/*
 *  Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.bench.generator;

import com.uber.hoodie.func.LazyIterableIterator;
import java.util.Iterator;
import org.apache.avro.generic.GenericRecord;

/**
 * A lazy record generator to generate {@link GenericRecord}s lazily and not hold a list of records in memory
 */
public class LazyRecordGeneratorIterator extends
    LazyIterableIterator<GenericRecord, GenericRecord> {

  public LazyRecordGeneratorIterator(Iterator<GenericRecord> inputItr) {
    super(inputItr);
  }

  @Override
  protected void start() {
  }

  @Override
  protected GenericRecord computeNext() {
    return inputItr.next();
  }

  @Override
  protected void end() {

  }
}
