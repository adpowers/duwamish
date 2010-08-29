package org.andrewhitchcock.duwamish;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.andrewhitchcock.duwamish.accumulator.BooleanAndAccumulator;
import org.andrewhitchcock.duwamish.accumulator.LongSumAccumulator;
import org.andrewhitchcock.duwamish.model.Accumulator;
import org.andrewhitchcock.duwamish.model.HaltDecider;
import org.andrewhitchcock.duwamish.model.Partitioner;
import org.andrewhitchcock.duwamish.model.Vertex;
import org.andrewhitchcock.duwamish.util.Accumulators;
import org.andrewhitchcock.duwamish.util.DefaultHaltDecider;
import org.andrewhitchcock.duwamish.util.HashPartitioner;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.io.Files;
import com.google.protobuf.Message;

@SuppressWarnings("unchecked")
public class Duwamish<C extends Vertex<V, E, M>, V extends Message, E extends Message, M extends Message> {
  private DuwamishContext<C, V, E, M> duwamishContext;
  
  private Map<String, Accumulator> accumulators;

  private List<Partition> partitions;
  private Partitioner partitioner;
  
  private HaltDecider haltDecider;
  
  private File tempDir;
  
  
  private void init(int partitionCount) {
    
    accumulators = Maps.newHashMap();
    accumulators.put(Accumulators.VERTEX_COUNT, new LongSumAccumulator());
    accumulators.put(Accumulators.EDGE_COUNT, new LongSumAccumulator());
    accumulators.put(Accumulators.MESSAGE_COUNT, new LongSumAccumulator());
    accumulators.put(Accumulators.VOTE_TO_HALT, new BooleanAndAccumulator());
    
    tempDir = new File("/Users/Andrew/tmp/duwamish/");
    if (tempDir.exists()) {
      try {
        Files.deleteRecursively(tempDir);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    tempDir.mkdirs();
    tempDir.deleteOnExit();

    this.partitions = Lists.newArrayListWithCapacity(partitionCount);
    for (int i = 0; i < partitionCount; i++) {
      partitions.add(new Partition<C, V, E, M>(duwamishContext, accumulators, tempDir, partitionCount, i));
    }
    
    this.partitioner = new HashPartitioner(partitions);
    for (int i = 0; i < partitionCount; i++) {
      partitions.get(i).setup(partitioner);
    }
  }
  
  public static Builder newBuilder() {
    return new Builder();
  }
  
  public static class Builder {
    private final Duwamish duwamish;
    private int partitionCount = 8;
    
    private Builder() {
      duwamish = new Duwamish();
      duwamish.duwamishContext = new DuwamishContext();
    }  
    
    public <C extends Vertex> Builder withVertex(Class<C> vertex) {
      duwamish.duwamishContext.vertexClass = vertex;
      return this;
    }
    
    public <V extends Message> Builder withVertexType(Class<V> vertexType) {
      duwamish.duwamishContext.vertexType = vertexType;
      return this;
    }
    
    public <E extends Message> Builder withEdgeType(Class<E> edgeType) {
      duwamish.duwamishContext.edgeType = edgeType;
      return this;
    }
    
    public <M extends Message> Builder withMessageType(Class<M> messageType) {
      duwamish.duwamishContext.messageType = messageType;
      return this;
    }
    
    public Builder withPartitionCount(int partitionCount) {
      this.partitionCount = partitionCount;
      return this;
    }
    
    public <C extends Vertex<V, E, M>, V extends Message, E extends Message, M extends Message> Duwamish<C, V, E, M> build() {
      duwamish.init(partitionCount);
      return duwamish;
    }
  }
  
  
  public void addAccumulator(String name, Accumulator accumulator) {
    accumulators.put(name, accumulator);
  }
  
  public void setHaltDecider(HaltDecider haltDecider) {
    this.haltDecider = haltDecider;
  }
  
  public void addVertex(String id, V value) {
    partitions.get(partitioner.getPartitionIdByVertex(id)).addVertex(id, value);
  }
  
  public void addEdge(String fromId, String toId, E value) {
    partitions.get(partitioner.getPartitionIdByVertex(fromId)).addEdge(fromId, toId, value);
  }
  
  public void run(int runCount) {
    int threadPoolSize = 8;

    try {
      firstRun(threadPoolSize);
    
      for (long superstepNumber = 0; superstepNumber < runCount; superstepNumber++) {
        before(threadPoolSize);
        
        Map<String, Object> accumulations = runSuperstep(threadPoolSize, superstepNumber);
         
        // Print accumulations
        System.out.println("Round: " + superstepNumber);
        for (Map.Entry<String, Object> entry : accumulations.entrySet()) {
          System.out.println("    " + entry.getKey() + ": " + entry.getValue());
        }
        System.out.println();
        
        after(threadPoolSize);
        
        if (decideToHalt(superstepNumber, accumulations)) {
          return;
        }
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private void firstRun(int threadPoolSize) throws InterruptedException, ExecutionException {
    List<Future<Object>> futures = Lists.newArrayList();
    
    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
    for (Partition partition : partitions) {
      futures.add(executor.submit(new FirstRunCallable(partition)));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    
    for (Future<Object> future : futures) {
      future.get(); // ensure we see all exceptions
    }
  }
  
  private void before(int threadPoolSize) throws InterruptedException, ExecutionException {
    List<Future<Object>> futures = Lists.newArrayList();
    
    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
    for (Partition partition : partitions) {
      futures.add(executor.submit(new BeforeCallable(partition)));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    
    for (Future<Object> future : futures) {
      future.get(); // ensure we see all exceptions
    }
  }
  
  private Map<String, Object> runSuperstep(int threadPoolSize, long superstepNumber) throws InterruptedException, ExecutionException {
    List<Future<Map<String, Object>>> futures = Lists.newArrayList();

    // Execute all the supersteps and save their accumulation result futures
    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
    for (Partition partition : partitions) {
      futures.add(executor.submit(new SuperstepCallable(partition, superstepNumber)));
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
  
  private void after(int threadPoolSize) throws InterruptedException, ExecutionException {
    List<Future<Object>> futures = Lists.newArrayList();
    
    ExecutorService executor = Executors.newFixedThreadPool(threadPoolSize);
    for (Partition partition : partitions) {
      futures.add(executor.submit(new AfterCallable(partition)));
    }
    executor.shutdown();
    executor.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
    
    for (Future<Object> future : futures) {
      future.get(); // ensure we see all exceptions
    }
  }
  
  private boolean decideToHalt(long superstepNumber, Map<String, Object> accumulations) {
    if (haltDecider != null) {
      if (haltDecider.shouldHalt(superstepNumber, accumulations)) {
        return true;
      }
    }
    return new DefaultHaltDecider().shouldHalt(superstepNumber, accumulations);
  }
  
  
  private static class FirstRunCallable implements Callable<Object> {
    Partition partition;
    
    public FirstRunCallable(Partition partition) {
      this.partition = partition;
    }
    
    @Override
    public Object call() {
      partition.prepareForFirstRun();
      return null;
    }
  }
  
  private static class BeforeCallable implements Callable<Object> {
    Partition partition;
    
    public BeforeCallable(Partition partition) {
      this.partition = partition;
    }
    
    @Override
    public Object call() {
      partition.before();
      return null;
    }
  }
  
  private static class SuperstepCallable implements Callable<Map<String, Object>> {
    Partition partition;
    long superstepNumber;
    
    public SuperstepCallable(Partition partition, long superstepNumber) {
      this.partition = partition;
      this.superstepNumber = superstepNumber;
    }
    
    @Override
    public Map<String, Object> call() {
      return partition.runSuperstep(superstepNumber);
    }
  }
  
  private static class AfterCallable implements Callable<Object> {
    Partition partition;
    
    public AfterCallable(Partition partition) {
      this.partition = partition;
    }
    
    @Override
    public Object call() throws Exception {
      partition.after();
      return null;
    }
  }
  
  static class DuwamishContext<C extends Vertex<V, E, M>, V extends Message, E extends Message, M extends Message> {
    Class<C> vertexClass;
    
    Class<V> vertexType;
    Class<E> edgeType;
    Class<M> messageType;
  }
}
