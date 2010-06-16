package org.andrewhitchcock.duwamish;

import java.util.List;
import java.util.Map;
import java.util.Random;

import com.google.common.collect.Lists;

public class PageRank {
  public static void main(String[] args) throws Exception {
    Random random = new Random();
    
    final int vertexCount = 4096;
    final int maxEdgeCountPerVertex = 128;
    final int runCount = 200;
    
    class PageRankVertex extends Vertex<Double, Object, Double> {
      Double pageRank = 1.0;
      
      public PageRankVertex(String vertexId) {
        super(vertexId);
      }

      @Override
      public void compute(Iterable<Double> messages, Context<Double, Object, Double> context) {
        double originalPageRank = pageRank;
        
        // Sum incoming messages and adjust our page rank accordingly
        if (context.getSuperstepNumber() > 0) {
          double sum = 0;
          for (Double message : messages) {
            sum += message;
          } 
          pageRank = 0.15 + 0.85 * sum;
        }
        
        // Send page rank to our neighbors
        if (context.getSuperstepNumber() < runCount) {
          List<Edge<Object>> outEdges = Lists.newArrayList(context.getEdgeIterable());
          
          double outValue = pageRank / outEdges.size();
          for (Edge<Object> outEdge : outEdges) {
            context.sendMessageTo(outEdge.getTargetVertexId(), outValue);
          }
        }
        
        context.emitAccumulation("PageRankChange", Math.abs(originalPageRank - pageRank));
        context.emitAccumulation("MaxPageRank", pageRank);
        context.emitAccumulation("MinPageRank", pageRank);
        
        if (context.getSuperstepNumber() > 0) {
          double percentChange = (pageRank - originalPageRank) / originalPageRank;
          if (Math.abs(percentChange) < 0.00001) {
            context.voteToHalt();
          }
        }
        
        if (getVertexId().equals("20")) {
          System.out.println("PageRank: " + pageRank);
        }
      }
    }

    Duwamish<Double, Object, Double> duwamish = Duwamish.createWithPartitionCount(32);
    duwamish.addAccumulator("PageRankChange", new DoubleSumAccumulator());
    duwamish.addAccumulator("MaxPageRank", new DoubleMaxAccumulator());
    duwamish.addAccumulator("MinPageRank", new DoubleMinAccumulator());
    duwamish.setHaltDecider(new HaltDecider() {
      @Override
      public boolean shouldHalt(long superstepNumber, Map<String, Object> accumulations) {
        return (Boolean)accumulations.get(Accumulators.VOTE_TO_HALT);
      }
    });
    
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
