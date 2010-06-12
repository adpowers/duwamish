package org.andrewhitchcock.duwamish;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Lists;

public class Duwamish<V, E, M> {
  private List<Partition<V, E, M>> partitions;
  private Partitioner<V, E, M> partitioner;
  
  private Duwamish(int partitionCount) {
    this.partitions = Lists.newArrayListWithCapacity(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      partitions.add(new Partition<V, E, M>());
    }
    
    this.partitioner = new HashPartitioner<V, E, M>(partitions);
    for (int i = 0; i < partitionCount; i++) {
      partitions.get(i).setup(partitioner);
    }
  }
  
  public static <V, E, M> Duwamish<V, E, M> createWithPartitionCount(int partitionCount) {
    return new Duwamish<V, E, M>(partitionCount);
  }
  
  public void addVertex(Vertex<V, E, M> vertex) {
    partitioner.getPartitionByVertex(vertex.getVertexId()).addVertex(vertex);
  }
  
  public void addEdge(String vertexId, Edge<E> edge) {
    partitioner.getPartitionByVertex(vertexId).addEdge(vertexId, edge);
  }
  
  public void run(int runCount) {
    int threadPoolSize = 8;
    try {
      for (long i = 0; i < runCount; i++) {
        ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
        for (Partition<V, E, M> partition : partitions) {
          executor.execute(new PrepareRunnable<V, E, M>(partition));
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
        
        executor = Executors.newFixedThreadPool(threadPoolSize);
        for (Partition<V, E, M> partition : partitions) {
          executor.execute(new SuperstepRunnable<V, E, M>(partition, i));
        }
        executor.shutdown();
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
      }
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
  
  private static class PrepareRunnable<V, E, M> implements Runnable {
    Partition<V, E, M> partition;
    
    public PrepareRunnable(Partition<V, E, M> partition) {
      this.partition = partition;
    }
    
    @Override
    public void run() {
      partition.prepare();
    }
  }
  
  private static class SuperstepRunnable<V, E, M> implements Runnable {
    Partition<V, E, M> partition;
    long superstepNumber;
    
    public SuperstepRunnable(Partition<V, E, M> partition, long superstepNumber) {
      this.partition = partition;
      this.superstepNumber = superstepNumber;
    }
    
    @Override
    public void run() {
      partition.runSuperstep(superstepNumber);
    }
  }
}
