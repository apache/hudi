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

package org.apache.hudi.common.util.sorter;

import org.apache.hudi.common.util.DefaultSizeEstimator;
import org.apache.hudi.common.util.SampleEstimator;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.io.IOException;

@BenchmarkMode(Mode.AverageTime)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@State(Scope.Benchmark)
@Threads(Threads.MAX)
@Fork(1)
public class ExternalSorterSampleBenchmark extends ExternalSorterBenchmarkHarness {

  public static void main(String[] args) throws RunnerException {
    Options opt = new OptionsBuilder()
        .include(ExternalSorterSampleBenchmark.class.getSimpleName())
        .result("result.json")
        .resultFormat(ResultFormatType.JSON).build();
    new Runner(opt).run();
  }

  @Setup(Level.Trial)
  public void setUp() {
    prepare();
    sizeEstimator = new DefaultSizeEstimator<>();
  }

  @Param({"1", "10", "100", "1000", "10000", "100000"})
  private int triggerSampleCount;

  @Benchmark
  public void benchExternalSorter() throws IOException {
    long maxMemory = totalSize * 10;
    SampleEstimator sampleEstimator = new SampleEstimator(sizeEstimator, triggerSampleCount);
    ExternalSorter<Record> sorter =
        ExternalSorterFactory.create(ExternalSorterType.SORT_ON_READ, tmpDir.getPath(), maxMemory, Record::compareTo, sizeEstimator, SortEngine.HEAP);
    sorter.addAll(records.iterator());
    System.out.println("sample count: " + sampleEstimator.getSampleCount());
    sorter.finish();
    sorter.close();
  }

}
