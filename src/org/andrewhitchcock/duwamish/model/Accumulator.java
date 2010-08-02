package org.andrewhitchcock.duwamish.model;

public interface Accumulator<A> {
  public abstract A accumulate(Iterable<A> values);
}
