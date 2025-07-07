package org.apache.hudi;

import org.apache.hudi.common.util.ValidationUtils;

import java.io.Serializable;
import java.util.List;

public class Comparables implements Comparable, Serializable {
  protected static final long serialVersionUID = 1L;

  private List<Comparable> comparables;

  public Comparables(List<Comparable> comparables) {
    this.comparables = comparables;
  }

  @Override
  public int compareTo(Object o) {
    ValidationUtils.checkArgument(o instanceof Comparables, "Comparables can only be compared with another Comparables");
    Comparables otherComparables = (Comparables) o;
    ValidationUtils.checkArgument(comparables.size() == otherComparables.comparables.size(), "Comparables should be of same size");
    for (int i = 0; i < comparables.size(); i++) {
      int comparingValue = comparables.get(i).compareTo(otherComparables.comparables.get(i));
      if (comparingValue != 0) {
        return comparingValue;
      }
    }
    return 0;
  }
}
