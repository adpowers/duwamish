package org.andrewhitchcock.duwamish;

import java.util.List;

public class HashPartitioner<V, E, M> extends Partitioner<V, E, M> {
  private List<Partition<V, E, M>> partitions;
  
  public HashPartitioner(List<Partition<V, E, M>> partitions) { 
    this.partitions = partitions;
  }
  
  @Override
  public Partition<V, E, M> getPartitionByVertex(String vertexId) {
    return partitions.get(vertexId.hashCode() % partitions.size());
  }
}
