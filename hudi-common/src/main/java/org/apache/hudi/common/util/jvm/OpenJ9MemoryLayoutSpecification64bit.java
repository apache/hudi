package org.apache.hudi.common.util.jvm;

public class OpenJ9MemoryLayoutSpecification64bit implements MemoryLayoutSpecification {
  @Override
  public int getArrayHeaderSize() {
    return 16;
  }

  @Override
  public int getObjectHeaderSize() {
    return 16;
  }

  @Override
  public int getObjectPadding() {
    return 8;
  }

  @Override
  public int getReferenceSize() {
    return 8;
  }

  @Override
  public int getSuperclassFieldPadding() {
    return 8;
  }
}
