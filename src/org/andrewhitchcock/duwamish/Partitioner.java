package org.andrewhitchcock.duwamish;

public abstract class Partitioner<V, E, M> {
  public abstract Partition<V, E, M> getPartitionByVertex(String vertexId);
}
