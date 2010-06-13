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
  
  @SuppressWarnings("unchecked")
  private Map<String, Accumulator> accumulators;
  private Partitioner<V, E, M> partitioner;
  
  @SuppressWarnings("unchecked")
  public Partition(Map<String, Accumulator> accumulators) {
    this.accumulators = accumulators;
  }
  
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
  
  public Map<String, Object> runSuperstep(long superstepNumber) {
    Multimap<String, Object> accumulationMessages = ArrayListMultimap.create();
    
    // For each vertex, feed it its messages.
    for (Vertex<V, E, M> vertex : vertexes.values()) {
      Context<V, E, M> context = new Context<V, E, M>(superstepNumber, partitioner, accumulationMessages);
      Iterable<M> messagesIterable = CountingIterable.create(context, Accumulators.MESSAGE_COUNT, previousRoundMessages.removeAll(vertex.getVertexId()));
      Iterable<Edge<E>> edgeIterable = CountingIterable.create(context, Accumulators.EDGE_COUNT, edges.get(vertex.getVertexId()));
      context.setEdgeIterable(edgeIterable);    
      context.emitAccumulation(Accumulators.VERTEX_COUNT, 1L);
      vertex.compute(messagesIterable, context);
    }
    
    // Clean up previous messages
    previousRoundMessages.clear();
    
    // Accumulate
    return Accumulators.getAccumulations(accumulators, accumulationMessages);
  }
  
  private static class CountingIterable<V, E, M, T> implements Iterable<T> {
    final Context<V, E, M> context;
    final String name;
    final Iterator<T> wrapped;
    
    public CountingIterable(Context<V, E, M> context, String name, Iterable<T> wrapped) {
      this.context = context;
      this.name = name;
      this.wrapped = wrapped.iterator();
    }
    
    public static <V, E, M, T> CountingIterable <V, E, M, T> create(Context<V, E, M> context, String name, Iterable<T> wrapped) {
      return new CountingIterable<V, E, M, T>(context, name, wrapped);
    }
    
    @Override
    public Iterator<T> iterator() {
      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          return wrapped.hasNext();
        }

        @Override
        public T next() {
          T next = wrapped.next();
          context.emitAccumulation(name, 1L);
          return next;
        }

        @Override
        public void remove() {
          wrapped.remove();
        }
      };
    }

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
