package org.andrewhitchcock.duwamish;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;

public class Duwamish<V, E, M> {
  @SuppressWarnings("unchecked")
  private Map<String, Accumulator> accumulators;

  private List<Partition<V, E, M>> partitions;
  private Partitioner<V, E, M> partitioner;
  
  
  private Duwamish(int partitionCount) {
    accumulators = Maps.newHashMap();
    accumulators.put(Accumulators.VERTEX_COUNT, new LongSumAccumulator());
    accumulators.put(Accumulators.EDGE_COUNT, new LongSumAccumulator());
    accumulators.put(Accumulators.MESSAGE_COUNT, new LongSumAccumulator());
    accumulators.put(Accumulators.VOTE_TO_HALT, new BooleanAndAccumulator());

    this.partitions = Lists.newArrayListWithCapacity(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      partitions.add(new Partition<V, E, M>(accumulators));
    }
    
    this.partitioner = new HashPartitioner<V, E, M>(partitions);
    for (int i = 0; i < partitionCount; i++) {
      partitions.get(i).setup(partitioner);
    }
  }
  
  public static <V, E, M> Duwamish<V, E, M> createWithPartitionCount(int partitionCount) {
    return new Duwamish<V, E, M>(partitionCount);
  }
  
  @SuppressWarnings("unchecked")
  public void addAccumulator(String name, Accumulator accumulator) {
    accumulators.put(name, accumulator);
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
        preparePartitions(threadPoolSize);
        
        Map<String, Object> results = runSuperstep(threadPoolSize, i);
        
        // Print accumulations
        System.out.println("Round: " + i);
        for (Map.Entry<String, Object> entry : results.entrySet()) {
          System.out.println("    " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private void preparePartitions(int threadPoolSize) throws InterruptedException {
    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
    for (Partition<V, E, M> partition : partitions) {
      executor.submit(new PrepareCallable<V, E, M>(partition));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
  }
  
  private Map<String, Object> runSuperstep(int threadPoolSize, long superstepNumber) throws InterruptedException, ExecutionException {
    List<Future<Map<String, Object>>> futures = Lists.newArrayList();

    // Execute all the supersteps and save their accumulation result futures
    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
    for (Partition<V, E, M> partition : partitions) {
      futures.add(executor.submit(new SuperstepCallable<V, E, M>(partition, superstepNumber)));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    
    // Convert from a list of map futures to a multimap
    Multimap<String, Object> accumulationMessages = ArrayListMultimap.create();
    for (Future<Map<String, Object>> future : futures) {
      Map<String, Object> results = future.get();
      for (Map.Entry<String, Object> entry : results.entrySet()) {
        accumulationMessages.put(entry.getKey(), entry.getValue());
      }
    }
    return Accumulators.getAccumulations(accumulators, accumulationMessages);
  }
  
  
  private static class PrepareCallable<V, E, M> implements Callable<Object> {
    Partition<V, E, M> partition;
    
    public PrepareCallable(Partition<V, E, M> partition) {
      this.partition = partition;
    }
    
    @Override
    public Object call() {
      partition.prepare();
      return null;
    }
  }
  
  
  private static class SuperstepCallable<V, E, M> implements Callable<Map<String, Object>> {
    Partition<V, E, M> partition;
    long superstepNumber;
    
    public SuperstepCallable(Partition<V, E, M> partition, long superstepNumber) {
      this.partition = partition;
      this.superstepNumber = superstepNumber;
    }
    
    @Override
    public Map<String, Object> call() {
      return partition.runSuperstep(superstepNumber);
    }
  }
}
