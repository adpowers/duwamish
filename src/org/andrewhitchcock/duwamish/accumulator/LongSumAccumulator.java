package org.andrewhitchcock.duwamish.accumulator;

import org.andrewhitchcock.duwamish.model.Accumulator;

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
