package org.andrewhitchcock.duwamish;

import com.google.common.collect.Multimap;

public class Context<V, E, M> {
  private long superstepNumber;
  private Iterable<Edge<E>> edgeIterable;
  private Partitioner<V, E, M> partitioner;
  private Multimap<String, Object> accumulationMessages;
  
  public Context(long superstepNumber, Partitioner<V, E, M> partitioner, Multimap<String, Object> accumulationMessages) {
    this.superstepNumber = superstepNumber;
    this.partitioner = partitioner;
    this.accumulationMessages = accumulationMessages;
  }
  
  public void setEdgeIterable(Iterable<Edge<E>> edgeIterable) {
    this.edgeIterable = edgeIterable;
  }
  
  public long getSuperstepNumber() {
    return superstepNumber;
  }
  
  public Iterable<Edge<E>> getEdgeIterable() {
    return edgeIterable;
  }
  
  public void sendMessageTo(String vertexId, M message) {
    partitioner.getPartitionByVertex(vertexId).sendMessage(vertexId, message);
  }
  
  public void voteToHalt() {
    emitAccumulation(Accumulators.VOTE_TO_HALT, Boolean.TRUE);
  }
  
  public void emitAccumulation(String name, Object value) {
    accumulationMessages.put(name, value);
  }
}
