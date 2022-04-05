package org.apache.hudi.util;

import java.util.function.Supplier;

// TODO java-doc
public class LazyRef<T> {

  private volatile boolean initialized;

  private Supplier<T> initializer;
  private T ref;

  private LazyRef(Supplier<T> initializer) {
    this.initializer = initializer;
    this.ref = null;
    this.initialized = false;
  }

  private LazyRef(T ref) {
    this.initializer = null;
    this.ref = ref;
    this.initialized = true;
  }

  public T get() {
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          this.ref = initializer.get();
          this.initializer = null;
          initialized = true;
        }
      }
    }

    return ref;
  }

  public static <T> LazyRef<T> lazy(Supplier<T> initializer) {
    return new LazyRef<>(initializer);
  }

  public static <T> LazyRef<T> eager(T ref) {
    return new LazyRef<>(ref);
  }
}
