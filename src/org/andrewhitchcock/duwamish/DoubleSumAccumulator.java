package org.andrewhitchcock.duwamish;

public class DoubleSumAccumulator implements Accumulator<Double> {
  @Override
  public Double accumulate(Iterable<Double> values) {
    double total = 0;
    for (Double value : values) {
      total += value;
    }
    return total;
  }
}
