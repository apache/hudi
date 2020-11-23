package org.apache.hudi.common.util.jvm;

public class HotSpotMemoryLayoutSpecification64bitCompressed implements MemoryLayoutSpecification {
  @Override
  public int getArrayHeaderSize() {
    return 16;
  }

  @Override
  public int getObjectHeaderSize() {
    return 12;
  }

  @Override
  public int getObjectPadding() {
    return 8;
  }

  @Override
  public int getReferenceSize() {
    return 4;
  }

  @Override
  public int getSuperclassFieldPadding() {
    return 4;
  }
}
