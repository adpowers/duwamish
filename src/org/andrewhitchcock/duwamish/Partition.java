package org.andrewhitchcock.duwamish;

import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class Partition<V, E, M> {
  private Queue<Message<M>> incomingMessages = new ConcurrentLinkedQueue<Message<M>>();
  private Multimap<String, M> previousRoundMessages = ArrayListMultimap.create();
  private Map<String, Vertex<V, E, M>> vertexes = Maps.newHashMap();
  private Multimap<String, Edge<E>> edges = ArrayListMultimap.create();
  
  private Partitioner<V, E, M> partitioner;
  
  
  public void addVertex(Vertex<V, E, M> vertex) {
    vertexes.put(vertex.getVertexId(), vertex);
  }
  
  public void addEdge(String vertexId, Edge<E> edge) {
    edges.put(vertexId, edge);
  }
  
  public void sendMessage(String vertexId, M message) {
    incomingMessages.add(new Message<M>(vertexId, message));
  }
  
  public void setup(Partitioner<V, E, M> partitioner) {
    this.partitioner = partitioner;
  }
  
  public void prepare() {
    // Move incoming messages to previousRoundMessages (aka, bucket sort)
    int count = 0;
    while (!incomingMessages.isEmpty()) {
      Message<M> message = incomingMessages.poll();
      previousRoundMessages.put(message.vertexId, message.message);
      count++;
    }
  }
  
  public boolean runSuperstep(long superstepNumber) {    
    boolean votedToHalt = true;
    
    // For each vertex, feed it its messages.
    for (Vertex<V, E, M> vertex : vertexes.values()) {
      Iterator<M> messagesIterator = previousRoundMessages.removeAll(vertex.getVertexId()).iterator();
      Iterator<Edge<E>> edgeIterator = edges.get(vertex.getVertexId()).iterator();
      Context<V, E, M> context = new Context<V, E, M>(superstepNumber, edgeIterator, partitioner);
      
      vertex.compute(messagesIterator, context);
      
      votedToHalt &= context.getVotedToHalt();
    }
    
    // Clean up
    previousRoundMessages.clear();
    
    return votedToHalt;
  }
  
  private static class Message<M> {
    private String vertexId;
    private M message;
    
    public Message(String vertexId, M message) {
      this.vertexId = vertexId;
      this.message = message;
    }
  }
}
