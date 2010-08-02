package org.andrewhitchcock.duwamish.accumulator;

import org.andrewhitchcock.duwamish.model.Accumulator;

public class BooleanAndAccumulator implements Accumulator<Boolean> {
  @Override
  public Boolean accumulate(Iterable<Boolean> values) {
    boolean seenValue = false;
    boolean andResults = true;
    for (Boolean value : values) {
      seenValue = true;
      andResults &= value;
    }
    return seenValue & andResults;
  }
}
