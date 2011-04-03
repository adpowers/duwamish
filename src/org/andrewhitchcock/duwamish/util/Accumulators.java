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

package org.andrewhitchcock.duwamish.util;

import java.util.Map;

import org.andrewhitchcock.duwamish.model.Accumulator;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class Accumulators {
  public static final String VERTEX_COUNT = "VertexCount";
  public static final String EDGE_COUNT = "EdgeCount";
  public static final String MESSAGE_COUNT = "MessageCount";
  public static final String VOTE_TO_HALT = "VoteToHalt";
  
  @SuppressWarnings("unchecked")
  public static Map<String, Object> getAccumulations(Map<String, Accumulator> accumulators, Multimap<String, Object> accumulationMessages) {
    Map<String, Object> results = Maps.newHashMap();
    for (Map.Entry<String, Accumulator> entry : accumulators.entrySet()) {
      String name = entry.getKey();
      Accumulator accumulator = entry.getValue();
      results.put(name, accumulator.accumulate(accumulationMessages.get(name)));
    }
    return results;
  }
}
