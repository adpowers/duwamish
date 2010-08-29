package org.andrewhitchcock.duwamish;

import org.andrewhitchcock.duwamish.model.Edge;

import com.google.common.collect.Multimap;
import com.google.protobuf.Message;

@SuppressWarnings("unchecked")
public class Context<V extends Message, E extends Message, M extends Message> {
  private long superstepNumber;
  private Iterable<Edge<E>> edgeIterable;
  private Partition partition;
  private Multimap<String, Object> accumulationMessages;
  private boolean votedToHalt = false;
  
  public Context(long superstepNumber, Partition partition, Multimap<String, Object> accumulationMessages) {
    this.superstepNumber = superstepNumber;
    this.partition = partition;
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
    partition.sendMessage(vertexId, message);
  }
  
  public void voteToHalt() {
    votedToHalt = true;
  }
  
  public boolean getVotedToHalt() {
    return votedToHalt;
  }
  
  public void emitAccumulation(String name, Object value) {
    accumulationMessages.put(name, value);
  }
}
