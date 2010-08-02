package org.andrewhitchcock.duwamish.model;

import org.andrewhitchcock.duwamish.Context;

public abstract class Vertex<V, E, M> {
  private String vertexId;
  private V value;
  
  public Vertex(String vertexId) {
    this.vertexId = vertexId;
  }
  
  public abstract void compute(Iterable<M> messages, Context<V, E, M> context);
  
  public String getVertexId() {
    return vertexId;
  }
  
  public V getValue() {
    return value;
  }
}
