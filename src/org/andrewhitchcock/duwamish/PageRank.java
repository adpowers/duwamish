package org.andrewhitchcock.duwamish;

import java.util.Iterator;
import java.util.List;
import java.util.Random;

import com.google.common.collect.Lists;

public class PageRank {
  public static void main(String[] args) throws Exception {
    Random random = new Random();
    
    final int vertexCount = 4096;
    final int maxEdgeCountPerVertex = 128;
    final int runCount = 200;
    
    class PageRankVertex extends Vertex<Double, Object, Double> {
      Double pageRank = 10.0;
      
      public PageRankVertex(String vertexId) {
        super(vertexId);
      }

      @Override
      public void compute(Iterator<Double> messages, Context<Double, Object, Double> context) {
        // Sum incoming messages and adjust our page rank accordingly
        if (context.getSuperstepNumber() > 0) {
          double sum = 0;
          while (messages.hasNext()) {
            sum += messages.next();
          } 
          pageRank = 0.15 / vertexCount + 0.85 * sum;
        }
        
        // Send page rank to our neighbors
        if (context.getSuperstepNumber() < runCount) {
          List<Edge<Object>> outEdges = Lists.newArrayList(context.getEdgeIterator());
          
          double outValue = pageRank / outEdges.size();
          for (Edge<Object> outEdge : outEdges) {
            context.sendMessageTo(outEdge.getTargetVertexId(), outValue);
          }
        }
        
        if (getVertexId().equals("20")) {
          System.out.println("PageRank: " + pageRank);
        }
      }
    }

    Duwamish<Double, Object, Double> duwamish = Duwamish.createWithPartitionCount(32);
    
    // Setup vertexes and edges
    for (int i = 0; i < vertexCount; i++) {
      String id = Integer.toString(i);
      PageRankVertex pageRank = new PageRankVertex(id);
      duwamish.addVertex(pageRank);
      
      int outEdges = random.nextInt(maxEdgeCountPerVertex);
      for (int j = 0; j < outEdges; j++) {
        Edge<Object> edge = new Edge<Object>(Integer.toString(random.nextInt(vertexCount)), null);
        duwamish.addEdge(id, edge);
      }
    }
    
    duwamish.run(runCount);
  }
}
