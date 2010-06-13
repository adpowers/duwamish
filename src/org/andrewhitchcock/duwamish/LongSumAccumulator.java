package org.andrewhitchcock.duwamish;

public class LongSumAccumulator implements Accumulator<Long> {
  @Override
  public Long accumulate(Iterable<Long> values) {
    long total = 0;
    for (Long value : values) {
      total += value;
    }
    return total;
  }
}
