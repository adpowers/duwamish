package org.andrewhitchcock.duwamish;

import java.util.Iterator;

public abstract class Vertex<V, E, M> {
  private String vertexId;
  private V value;
  
  public Vertex(String vertexId) {
    this.vertexId = vertexId;
  }
  
  public abstract void compute(Iterator<M> messages, Context<V, E, M> context);
  
  public String getVertexId() {
    return vertexId;
  }
  
  public V getValue() {
    return value;
  }
}
