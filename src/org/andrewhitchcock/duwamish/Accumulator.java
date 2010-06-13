package org.andrewhitchcock.duwamish;

public interface Accumulator<A> {
  public abstract A accumulate(Iterable<A> values);
}
