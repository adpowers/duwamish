package org.andrewhitchcock.duwamish;

public class DoubleMinAccumulator implements Accumulator<Double> {
  @Override
  public Double accumulate(Iterable<Double> values) {
    double min = Double.MAX_VALUE;
    for (Double value : values) {
      min = Math.min(value, min);
    }
    return min;
  }
}
