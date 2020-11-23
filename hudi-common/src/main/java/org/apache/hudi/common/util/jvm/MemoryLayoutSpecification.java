package org.apache.hudi.common.util.jvm;

/**
 * Describes constant memory overheads for various constructs in a JVM implementation.
 */
public interface MemoryLayoutSpecification {

  /**
   * Returns the fixed overhead of an array of any type or length in this JVM.
   *
   * @return the fixed overhead of an array.
   */
  int getArrayHeaderSize();

  /**
   * Returns the fixed overhead of for any {@link Object} subclass in this JVM.
   *
   * @return the fixed overhead of any object.
   */
  int getObjectHeaderSize();

  /**
   * Returns the quantum field size for a field owned by an object in this JVM.
   *
   * @return the quantum field size for an object.
   */
  int getObjectPadding();

  /**
   * Returns the fixed size of an object reference in this JVM.
   *
   * @return the size of all object references.
   */
  int getReferenceSize();

  /**
   * Returns the quantum field size for a field owned by one of an object's ancestor superclasses in this JVM.
   *
   * @return the quantum field size for a superclass field.
   */
  int getSuperclassFieldPadding();
}
