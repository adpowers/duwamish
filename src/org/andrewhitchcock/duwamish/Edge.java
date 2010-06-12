package org.andrewhitchcock.duwamish;

public class Edge<E> {
  private String targetVertexId;
  private E value;
  
  public Edge(String endpointVertexId, E value) {
    this.targetVertexId = endpointVertexId;
    this.value = value;
  }
  
  public String getTargetVertexId() {
    return targetVertexId;
  }
  
  public E getValue() {
    return value;
  }
}
