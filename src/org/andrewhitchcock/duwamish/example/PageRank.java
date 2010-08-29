package org.andrewhitchcock.duwamish.example;

import java.util.List;
import java.util.Map;
import java.util.Random;

import org.andrewhitchcock.duwamish.Context;
import org.andrewhitchcock.duwamish.Duwamish;
import org.andrewhitchcock.duwamish.accumulator.DoubleMaxAccumulator;
import org.andrewhitchcock.duwamish.accumulator.DoubleMinAccumulator;
import org.andrewhitchcock.duwamish.accumulator.DoubleSumAccumulator;
import org.andrewhitchcock.duwamish.example.protos.Examples.DoubleMessage;
import org.andrewhitchcock.duwamish.model.Edge;
import org.andrewhitchcock.duwamish.model.HaltDecider;
import org.andrewhitchcock.duwamish.model.Vertex;
import org.andrewhitchcock.duwamish.protos.Duwamish.EmptyMessage;
import org.andrewhitchcock.duwamish.util.Accumulators;

import com.google.common.collect.Lists;

public class PageRank {
  
  public static class PageRankVertex extends Vertex<DoubleMessage, EmptyMessage, DoubleMessage> {
    private int runCount = 200;
    
    @Override
    public DoubleMessage compute(String vertexId, DoubleMessage value, Iterable<DoubleMessage> messages, Context<DoubleMessage, EmptyMessage, DoubleMessage> context) {
      double originalPageRank = value.getValue();
      double pageRank = value.getValue();
      
      // Sum incoming messages and adjust our page rank accordingly
      if (context.getSuperstepNumber() > 0) {
        double sum = 0;
        for (DoubleMessage message : messages) {
          sum += message.getValue();
        } 
        pageRank = 0.15 + 0.85 * sum;
      }
      
      // Send page rank to our neighbors
      if (context.getSuperstepNumber() < runCount) {
        List<Edge<EmptyMessage>> outEdges = Lists.newArrayList(context.getEdgeIterable());
        double outValue = pageRank / outEdges.size();
        for (Edge<EmptyMessage> outEdge : outEdges) {
          context.sendMessageTo(outEdge.getTargetVertexId(), DoubleMessage.newBuilder().setValue(outValue).build());
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
      
      if (vertexId.equals("20")) {
        System.out.println("PageRank: " + pageRank);
      }
      
      return DoubleMessage.newBuilder().setValue(pageRank).build();
    }
  }
  
  public static void main(String[] args) throws Exception {
    Random random = new Random();

    int runCount = 200;
    int vertexCount = 4096;
    int maxEdgeCountPerVertex = 128;

    if (args.length == 2) {
      vertexCount = Integer.parseInt(args[0]);
      random.setSeed(Long.parseLong(args[1]));
    }

    Duwamish<PageRankVertex, DoubleMessage, EmptyMessage, DoubleMessage> duwamish = Duwamish.newBuilder()
      .withVertex(PageRankVertex.class)
      .withVertexType(DoubleMessage.class)
      .withEdgeType(EmptyMessage.class)
      .withMessageType(DoubleMessage.class)
      .build();
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
      duwamish.addVertex(id, DoubleMessage.newBuilder().setValue(1.0).build());
      
      int outEdges = random.nextInt(maxEdgeCountPerVertex);
      for (int j = 0; j < outEdges; j++) {
        String toId = Integer.toString(random.nextInt(vertexCount));
        duwamish.addEdge(id, toId, null);
      }
    }
    
    duwamish.run(runCount);
  }
}
