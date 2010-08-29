package org.andrewhitchcock.duwamish.util;

import java.util.List;

import org.andrewhitchcock.duwamish.Partition;
import org.andrewhitchcock.duwamish.model.Partitioner;

@SuppressWarnings("unchecked")
public class HashPartitioner extends Partitioner {
  private List<Partition> partitions;
  
  public HashPartitioner(List<Partition> partitions) { 
    this.partitions = partitions;
  }
  
  @Override
  public int getPartitionIdByVertex(String vertexId) {
    return Math.abs(vertexId.hashCode()) % partitions.size();
  }
}
