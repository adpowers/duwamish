package org.andrewhitchcock.duwamish.model;

import org.andrewhitchcock.duwamish.Partition;

public abstract class Partitioner<V, E, M> {
  public abstract Partition<V, E, M> getPartitionByVertex(String vertexId);
}
