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

package org.andrewhitchcock.duwamish.example;

import java.util.Random;

import org.andrewhitchcock.duwamish.Context;
import org.andrewhitchcock.duwamish.Duwamish;
import org.andrewhitchcock.duwamish.accumulator.LongSumAccumulator;
import org.andrewhitchcock.duwamish.example.protos.Examples.IntegerMessage;
import org.andrewhitchcock.duwamish.example.protos.Examples.ShortestPathPair;
import org.andrewhitchcock.duwamish.model.Edge;
import org.andrewhitchcock.duwamish.model.Vertex;

/**
 * Single source shortest path.
 */
public class ShortestPath {
  
  public static class ShortestPathVertex extends Vertex<ShortestPathPair, IntegerMessage, ShortestPathPair> {
    
    @Override
    public ShortestPathPair compute(String vertexId, ShortestPathPair value, Iterable<ShortestPathPair> messages, Context<ShortestPathPair, IntegerMessage, ShortestPathPair> context) {
      ShortestPathPair bestPair = value;
      boolean switchedPair = false;
      
      for (ShortestPathPair pair : messages) {
        if (bestPair == null || pair.getPathValue() < bestPair.getPathValue()) {
          bestPair = pair;
          switchedPair = true;
        }
      }
      
      if (switchedPair || (bestPair != null && context.getSuperstepNumber() == 0)) {
        for (Edge<IntegerMessage> outEdge : context.getEdgeIterable()) {
          context.sendMessageTo(
              outEdge.getTargetVertexId(),
              ShortestPathPair.newBuilder()
                .setFromVertex(vertexId)
                .setPathValue(bestPair.getPathValue() + outEdge.getValue().getValue())
                .build());
        }
      } else {
        context.voteToHalt();
      }
      
      if (bestPair != null) {
        context.emitAccumulation("NonNullCount", 1L);
        context.emitAccumulation("TotalValue", (long)bestPair.getPathValue());
      }
      
      if (vertexId.equals("20")) {
        if (bestPair == null) {
          System.out.println("From: null");
          System.out.println("Value: null");
        } else {
          System.out.println("From: " + bestPair.getFromVertex());
          System.out.println("Value: " + bestPair.getPathValue());
        }
      }
      
      return bestPair;
    }
  }
  
  public static void main(String[] args) {
    Random random = new Random();
    
    int vertexCount = 4096;
    int maxEdgeCountPerVertex = 128;    
    
    if (args.length == 2) {
      vertexCount = Integer.parseInt(args[0]);
      random.setSeed(Long.parseLong(args[1]));
    }
    
    
    
    Duwamish<ShortestPathVertex, ShortestPathPair, IntegerMessage, ShortestPathPair> duwamish = Duwamish.newBuilder()
      .withVertex(ShortestPathVertex.class)
      .withVertexType(ShortestPathPair.class)
      .withEdgeType(IntegerMessage.class)
      .withMessageType(ShortestPathPair.class)
      .build();
    duwamish.addAccumulator("NonNullCount", new LongSumAccumulator());
    duwamish.addAccumulator("TotalValue", new LongSumAccumulator());
    
    // Setup vertexes and edges
    for (int i = 0 ; i < vertexCount; i++) {
      String id = Integer.toString(i);
      if (i == 0) {
        duwamish.addVertex(id, ShortestPathPair.newBuilder().setFromVertex("0").setPathValue(0).build());
      } else {
        duwamish.addVertex(id, null);
      }
      
      int outEdges = random.nextInt(maxEdgeCountPerVertex);
      for (int j = 0; j < outEdges; j++) {
        duwamish.addEdge(
            id,
            Integer.toString(random.nextInt(vertexCount)),
            IntegerMessage.newBuilder().setValue(random.nextInt(16364)).build());
      }
    }
    
    duwamish.run(10000);
  }
}
