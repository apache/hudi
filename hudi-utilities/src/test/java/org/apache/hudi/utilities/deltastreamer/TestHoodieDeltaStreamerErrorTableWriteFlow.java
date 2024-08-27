package org.apache.hudi.utilities.deltastreamer;

import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.util.collection.Tuple3;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

public class TestHoodieDeltaStreamerErrorTableWriteFlow extends TestHoodieDeltaStreamerSchemaEvolutionExtensive {
  protected static Stream<Arguments> testErrorTableWriteFlowArgs() {
    Stream.Builder<Arguments> b = Stream.builder();
    // totalRecords, numErrorRecords, numSourceFiles, WriteOperationType, shouldWriteErrorTableInUnionWithBaseTable

    // empty source, error table union enabled
    b.add(Arguments.of(0, 0, 0, WriteOperationType.INSERT, true));
    // empty source, error table union disabled
    b.add(Arguments.of(0, 0, 0, WriteOperationType.INSERT, false));
    // non-empty source, error table union enabled
    b.add(Arguments.of(100, 5, 1, WriteOperationType.INSERT, true));
    // non-empty source, error table union disabled
    b.add(Arguments.of(100, 5, 1, WriteOperationType.INSERT, false));
    return b.build();
  }

  @ParameterizedTest
  @MethodSource("testErrorTableWriteFlowArgs")
  public void testErrorTableWriteFlow(
      int totalRecords,
      int numErrorRecords,
      int numSourceFiles,
      WriteOperationType wopType,
      boolean writeErrorTableInParallel) throws Exception {
    this.withErrorTable = true;
    this.writeErrorTableInParallelWithBaseTable = writeErrorTableInParallel;
    this.writeOperationType = wopType;
    this.useSchemaProvider = false;
    this.useTransformer = false;
    this.tableType = "COPY_ON_WRITE";
    this.shouldCluster = false;
    this.shouldCompact = false;
    this.rowWriterEnable = false;
    this.addFilegroups = false;
    this.multiLogFiles = false;
    this.dfsSourceLimitBytes = 100000000; // set source limit to 100mb
    testBase(Tuple3.of(true, totalRecords, numSourceFiles), numErrorRecords);
  }
}
