package org.andrewhitchcock.duwamish;

import java.util.Map;

import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class Accumulators {
  public static final String VERTEX_COUNT = "VertexCount";
  public static final String EDGE_COUNT = "EdgeCount";
  public static final String MESSAGE_COUNT = "MessageCount";
  public static final String VOTE_TO_HALT = "VoteToHalt";
  
  @SuppressWarnings("unchecked")
  static Map<String, Object> getAccumulations(Map<String, Accumulator> accumulators, Multimap<String, Object> accumulationMessages) {
    Map<String, Object> results = Maps.newHashMap();
    for (Map.Entry<String, Accumulator> entry : accumulators.entrySet()) {
      String name = entry.getKey();
      Accumulator accumulator = entry.getValue();
      results.put(name, accumulator.accumulate(accumulationMessages.get(name)));
    }
    return results;
  }
}
