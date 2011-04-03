/**
 * Copyright 2011 Andrew Hitchcock
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.andrewhitchcock.duwamish.accumulator;

import org.andrewhitchcock.duwamish.model.Accumulator;

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
