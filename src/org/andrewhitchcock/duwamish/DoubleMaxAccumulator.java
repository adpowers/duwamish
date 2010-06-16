package org.andrewhitchcock.duwamish;

public class DoubleMaxAccumulator implements Accumulator<Double> {
  @Override
  public Double accumulate(Iterable<Double> values) {
    double max = Double.MIN_VALUE;
    for (Double value : values) {
      max = Math.max(value, max);
    }
    return max;
  }
}
