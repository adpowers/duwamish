package org.andrewhitchcock.duwamish.example;

import java.util.Random;

import org.andrewhitchcock.duwamish.Context;
import org.andrewhitchcock.duwamish.Duwamish;
import org.andrewhitchcock.duwamish.accumulator.LongSumAccumulator;
import org.andrewhitchcock.duwamish.model.Edge;
import org.andrewhitchcock.duwamish.model.Vertex;

/**
 * Single source shortest path.
 */
public class ShortestPath {
  public static void main(String[] args) {
    Random random = new Random();
    
    int vertexCount = 4096;
    int maxEdgeCountPerVertex = 128;    
    
    if (args.length == 2) {
      vertexCount = Integer.parseInt(args[0]);
      random.setSeed(Long.parseLong(args[1]));
    }
    
    final class Pair {
      public final String fromVertex;
      public final Integer pathValue;
      
      public Pair(String fromVertex, Integer pathValue) {
        this.fromVertex = fromVertex;
        this.pathValue = pathValue;
      }
    }
    
    class ShortestPathVertex extends Vertex<Pair, Integer, Pair> {
      public ShortestPathVertex(String vertexId) {
        super(vertexId);
      }
      
      @Override
      public void compute(Iterable<Pair> messages, Context<Pair, Integer, Pair> context) {
        Pair bestPair = getValue();
        boolean switchedPair = false;
        
        for (Pair pair : messages) {
          if (bestPair == null || pair.pathValue < bestPair.pathValue) {
            bestPair = pair;
            switchedPair = true;
          }
        }
        
        if (switchedPair || (bestPair != null && context.getSuperstepNumber() == 0)) {
          for (Edge<Integer> outEdge : context.getEdgeIterable()) {
            context.sendMessageTo(outEdge.getTargetVertexId(), new Pair(getVertexId(), bestPair.pathValue + outEdge.getValue()));
          }
        } else {
          context.voteToHalt();
        }
        
        setValue(bestPair);
        
        if (bestPair != null) {
          context.emitAccumulation("NonNullCount", 1L);
          context.emitAccumulation("TotalValue", (long)bestPair.pathValue);
        }
        
        if (getVertexId().equals("20"))
          if (bestPair == null) {
            System.out.println("From: null");
            System.out.println("Value: null");
          } else {
            System.out.println("From: " + bestPair.fromVertex);
            System.out.println("Value: " + bestPair.pathValue);
          }
      }
    }
    
    Duwamish<Pair, Integer, Pair> duwamish = Duwamish.createWithPartitionCount(32);
    duwamish.addAccumulator("NonNullCount", new LongSumAccumulator());
    duwamish.addAccumulator("TotalValue", new LongSumAccumulator());
    
    // Setup vertexes and edges
    for (int i = 0 ; i < vertexCount; i++) {
      String id = Integer.toString(i);
      ShortestPathVertex vertex = new ShortestPathVertex(id);
      duwamish.addVertex(vertex);
      
      if (i == 0) {
        vertex.setValue(new Pair("0", 0));
      }
      
      int outEdges = random.nextInt(maxEdgeCountPerVertex);
      for (int j = 0; j < outEdges; j++) {
        Edge<Integer> edge = new Edge<Integer>(Integer.toString(random.nextInt(vertexCount)), random.nextInt(16364));
        duwamish.addEdge(id, edge);
      }
    }
    
    duwamish.run(10000);
  }
}
