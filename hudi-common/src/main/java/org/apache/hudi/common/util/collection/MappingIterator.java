package org.apache.hudi.common.util.collection;

import java.util.Iterator;
import java.util.function.Function;

// TODO java-docs
public class MappingIterator<I, O> implements Iterator<O> {

  private final Iterator<I> source;
  private final Function<I, O> mapper;

  public MappingIterator(Iterator<I> source, Function<I, O> mapper) {
    this.source = source;
    this.mapper = mapper;
  }

  @Override
  public boolean hasNext() {
    return source.hasNext();
  }

  @Override
  public O next() {
    return mapper.apply(source.next());
  }
}
