package org.apache.hudi.common.function;

import java.io.Serializable;
import java.util.function.Function;

/**
 * Serializable {@link Function} interface that only might be throwing unchecked exceptions
 *
 * @param <I> input type
 * @param <O> output type
 */
@FunctionalInterface
public interface SerializableFunctionUnchecked<I, O> extends Serializable {
  O apply(I v1);
}
