package org.andrewhitchcock.duwamish;

import java.util.Iterator;

public class Context<V, E, M> {
  private long superstepNumber;
  private Iterator<Edge<E>> edgeIterator;
  private Partitioner<V, E, M> partitioner;
  private boolean halted = false;
  
  public Context(long superstepNumber, Iterator<Edge<E>> edgeIterator, Partitioner<V, E, M> partitioner) {
    this.superstepNumber = superstepNumber;
    this.edgeIterator = edgeIterator;
    this.partitioner = partitioner;
  }
  
  public long getSuperstepNumber() {
    return superstepNumber;
  }
  
  public Iterator<Edge<E>> getEdgeIterator() {
    return edgeIterator;
  }
  
  public void sendMessageTo(String vertexId, M message) {
    partitioner.getPartitionByVertex(vertexId).sendMessage(vertexId, message);
  }
  
  public void voteToHalt() {
    halted = true;
  }
  
  public boolean getVotedToHalt() {
    return halted;
  }
}
