package org.andrewhitchcock.duwamish;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.andrewhitchcock.duwamish.Duwamish.DuwamishContext;
import org.andrewhitchcock.duwamish.model.Accumulator;
import org.andrewhitchcock.duwamish.model.Edge;
import org.andrewhitchcock.duwamish.model.Partitioner;
import org.andrewhitchcock.duwamish.model.Vertex;
import org.andrewhitchcock.duwamish.util.Accumulators;
import org.andrewhitchcock.duwamish.util.Comparators;
import org.andrewhitchcock.duwamish.util.FileUtil;
import org.andrewhitchcock.duwamish.util.MergeSorter;
import org.andrewhitchcock.duwamish.util.ProtocolBufferReader;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ForwardingMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

public class Partition<C extends Vertex<V, E, M>, V extends Message, E extends Message, M extends Message> {
  
  @SuppressWarnings("unchecked")
  private Map<String, Accumulator> accumulators;
  private Partitioner partitioner;
  
  private final C vertex;
  private final Method newVertexBuilder;
  private final Method newEdgeBuilder;
  private final Method newMessageBuilder;
  
  private final int partitionCount;
  private final int partitionNumber;
  
  private final File tempDir;
  private final File localTempDir;

  private final File edgeFile;
  private final File vertexFile;
  private final File messageFile;
  
  private final File newVertexFile;
  
  private OutputStream edgeFileWriter;
  private OutputStream vertexFileWriter;
  
  private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Vertex> vertexReader;
  private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Edge> edgeReader;
  private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Message> messageReader;
  
  private File[] messageFiles;
  private OutputStream[] messageFileWriters;
  
  @SuppressWarnings("unchecked")
  public Partition(DuwamishContext<C, V, E, M> duwamishContext, Map<String, Accumulator> accumulators, File tempDir, int partitionCount, int partitionNumber) {
    this.accumulators = accumulators;
    this.tempDir = tempDir;
    this.localTempDir = new File(tempDir, "partition-" + partitionNumber);
    this.partitionCount = partitionCount;
    this.partitionNumber = partitionNumber;
    this.edgeFile = new File(tempDir, "edge-" + partitionNumber);
    this.vertexFile = new File(tempDir, "vertex-" + partitionNumber);
    this.messageFile = new File(tempDir, "message-" + partitionNumber);
    this.newVertexFile = new File(tempDir, "new-vertex-" + partitionNumber);
    
    this.edgeFileWriter = FileUtil.newOutputStream(edgeFile);
    this.vertexFileWriter = FileUtil.newOutputStream(vertexFile);
    
    messageFiles = new File[partitionCount];
    for (int i = 0; i < partitionCount; i++) {
      messageFiles[i] = new File(tempDir, "message-" + partitionNumber + "-" + i);
    }
    messageFileWriters = new OutputStream[partitionCount];
    
    try {
      vertex = duwamishContext.vertexClass.newInstance();
      newVertexBuilder = duwamishContext.vertexType.getMethod("newBuilder");
      newEdgeBuilder = duwamishContext.edgeType.getMethod("newBuilder");
      newMessageBuilder = duwamishContext.messageType.getMethod("newBuilder");
    } catch (Exception e) {
      throw new RuntimeException(e);
    } 
  }
  
  public void addVertex(String id, V value) {
    try {
      org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.Builder builder =
        org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.newBuilder().setId(id);
      if (value != null) {
        builder.setValue(value.toByteString());
      }
      builder.build().writeDelimitedTo(vertexFileWriter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void addEdge(String fromId, String toId, E value) {
    try {
      org.andrewhitchcock.duwamish.protos.Duwamish.Edge.Builder builder =
        org.andrewhitchcock.duwamish.protos.Duwamish.Edge
          .newBuilder()
          .setFrom(fromId)
          .setTo(toId);
      if (value != null) {
        builder.setValue(value.toByteString());
      }
      builder.build().writeDelimitedTo(edgeFileWriter);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void prepareForFirstRun() {
    FileUtil.closeAll(edgeFileWriter, vertexFileWriter);
    
    File tempEdgeFile = new File(edgeFile.getAbsolutePath() + ".tmp");
    File tempVertexFile = new File(vertexFile.getAbsolutePath() + ".tmp");
    
    edgeFile.renameTo(tempEdgeFile);
    vertexFile.renameTo(tempVertexFile);
    
    MergeSorter.create(org.andrewhitchcock.duwamish.protos.Duwamish.Edge.class, Comparators.newEdgeComparator(), localTempDir).sort(edgeFile, tempEdgeFile);
    MergeSorter.create(org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.class, Comparators.newVertexComparator(), localTempDir).sort(vertexFile, tempVertexFile);
    
    tempEdgeFile.delete();
    tempVertexFile.delete();
  }
  
  public void sendMessage(String vertexId, M message) {
    try {
      int targetPartitionId = partitioner.getPartitionIdByVertex(vertexId);
      org.andrewhitchcock.duwamish.protos.Duwamish.Message.newBuilder()
        .setDestination(vertexId)
        .setValue(message.toByteString())
        .build().writeDelimitedTo(messageFileWriters[targetPartitionId]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void setup(Partitioner partitioner) {
    this.partitioner = partitioner;
  }
  
  public void before() {
    // Sort incoming messages
    List<File> incomingMessageFiles = Lists.newArrayList();
    for (int i = 0; i < partitionCount; i++) {
      File incomingMessageFile = new File(tempDir, "message-" + i + "-" + partitionNumber);
      if (incomingMessageFile.exists()) {
        incomingMessageFiles.add(incomingMessageFile);
      }
    }
    
    MergeSorter.create(
        org.andrewhitchcock.duwamish.protos.Duwamish.Message.class,
        Comparators.newMessageComparator(),
        localTempDir).sort(messageFile, incomingMessageFiles.toArray(new File[0]));
    
    for (File incomingMessageFile : incomingMessageFiles) {
      incomingMessageFile.delete();
    }
    
    if (!messageFile.exists()) {
      try {
        messageFile.createNewFile();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }
  
  public Map<String, Object> runSuperstep(long superstepNumber) {
    // Open readers
    vertexReader = ProtocolBufferReader.newReader(org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.class, FileUtil.newInputStream(vertexFile));
    edgeReader = ProtocolBufferReader.newReader(org.andrewhitchcock.duwamish.protos.Duwamish.Edge.class, FileUtil.newInputStream(edgeFile));
    messageReader = ProtocolBufferReader.newReader(org.andrewhitchcock.duwamish.protos.Duwamish.Message.class, FileUtil.newInputStream(messageFile));
    
    // Open new vertex value writer
    vertexFileWriter = FileUtil.newOutputStream(newVertexFile);
    
    // Open outgoing message writers
    for (int i = 0; i < messageFiles.length; i++) {
      messageFileWriters[i] = FileUtil.newOutputStream(messageFiles[i]);
    }
    
    Multimap<String, Object> accumulationMessages = CombiningMultimap.create(accumulators, 128);
    
    while (vertexReader.hasNext()) {
      org.andrewhitchcock.duwamish.protos.Duwamish.Vertex vertexMessage = vertexReader.next();
      String vertexId = vertexMessage.getId();
      V value = getTypeFromByteString(newVertexBuilder, vertexMessage.getValue());

      Context<V, E, M> context = new Context<V, E, M>(superstepNumber, this, accumulationMessages);
      Iterable<M> messageIterable = CountingIterable.create(context, Accumulators.MESSAGE_COUNT, new FilteredMessageIterator<M>(messageReader, vertexId));
      Iterable<Edge<E>> edgeIterable = CountingIterable.create(context, Accumulators.EDGE_COUNT, new FilteredEdgeIterator<E>(edgeReader, vertexId));
      context.setEdgeIterable(edgeIterable);
      
      V newValue = vertex.compute(vertexId, value, messageIterable, context);
      
      context.emitAccumulation(Accumulators.VERTEX_COUNT, 1L);
      context.emitAccumulation(Accumulators.VOTE_TO_HALT, context.getVotedToHalt());
      
      // Make sure all the messages and edges were consume.
      Iterator<M> messageIterator = messageIterable.iterator();
      while (messageIterator.hasNext()) {
        messageIterator.next();
      }
      
      Iterator<Edge<E>> edgeIterator = edgeIterable.iterator();
      while (edgeIterator.hasNext()) {
        edgeIterator.next();
      }
      
      // Write the new vertex value out
      try {
        org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.Builder builder =
          org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.newBuilder()
            .setId(vertexId);
        if (newValue != null) {
          builder.setValue(newValue.toByteString());
        }
        builder.build().writeDelimitedTo(vertexFileWriter);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    
    // Accumulate
    return Accumulators.getAccumulations(accumulators, accumulationMessages);
  }
  
  public void after() {
    FileUtil.closeAll(vertexReader, edgeReader, messageReader, vertexFileWriter);
    FileUtil.closeAll(messageFileWriters);
    
    vertexReader = null;
    edgeReader = null;
    messageReader = null;
    vertexFileWriter = null;
    messageFileWriters = new OutputStream[partitionCount];
    
    if (messageFile.exists()) {
      messageFile.delete();
    }
    if (vertexFile.exists()) {
      vertexFile.delete();
    }
    
    MergeSorter.create(org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.class, Comparators.newVertexComparator(), localTempDir).sort(vertexFile, newVertexFile);
    
    if (newVertexFile.exists()) {
      newVertexFile.delete();
    }
  }
  
  @SuppressWarnings("unchecked")
  private <T> T getTypeFromByteString(Method builderMethod, ByteString value) {
    if (value.isEmpty()) {
      return null;
    }
    
    try {
      Builder builder = (Builder) builderMethod.invoke(null);
      builder.mergeFrom(value);
      return (T) builder.build();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  
  @SuppressWarnings("unchecked")
  private static class CombiningMultimap<A, B> extends ForwardingMultimap<A, B> {
    private Multimap<A, B> backingMultimap;
    private Map<A, Accumulator> accumulators;
    private long numberOfEntriesBeforeCombining;
    
    public CombiningMultimap(Map<A, Accumulator> accumulators, long numberOfEntriesBeforeCombining) {
      this.backingMultimap = ArrayListMultimap.create();
      this.accumulators = accumulators;
      this.numberOfEntriesBeforeCombining = numberOfEntriesBeforeCombining;
    }
    
    @Override
    public boolean put(A key, B value) {
      backingMultimap.put(key, value);
      Collection<B> resultantCollection = backingMultimap.get(key);
      if (resultantCollection.size() >= numberOfEntriesBeforeCombining) {
        if (accumulators.containsKey(key)) {
          B newValue = (B)accumulators.get(key).accumulate(resultantCollection);
          backingMultimap.removeAll(key);
          backingMultimap.put(key, newValue);
        }
      }
      return true; // we don't really follow the contract anyway, so this doesn't matter
    }
    
    @Override
    protected Multimap<A, B> delegate() {
      return backingMultimap;
    }
    
    public static <A, B> CombiningMultimap<A, B> create(Map<A, Accumulator> accumulators, long numberOfEntriesBeforeCombining) {
      return new CombiningMultimap<A, B>(accumulators, numberOfEntriesBeforeCombining);
    }
  }
  
  private static class CountingIterable<V extends Message, E extends Message, M extends Message, T> implements Iterable<T> {
    final Context<V, E, M> context;
    final String name;
    final Iterator<T> wrapped;
    
    public CountingIterable(Context<V, E, M> context, String name, Iterator<T> wrapped) {
      this.context = context;
      this.name = name;
      this.wrapped = wrapped;
    }
    
    public static <V extends Message, E extends Message, M extends Message, T> CountingIterable <V, E, M, T> create(Context<V, E, M> context, String name, Iterator<T> wrapped) {
      return new CountingIterable<V, E, M, T>(context, name, wrapped);
    }
    
    @Override
    public Iterator<T> iterator() {
      return new Iterator<T>() {
        @Override
        public boolean hasNext() {
          return wrapped.hasNext();
        }

        @Override
        public T next() {
          T next = wrapped.next();
          context.emitAccumulation(name, 1L);
          return next;
        }

        @Override
        public void remove() {
          wrapped.remove();
        }
      };
    }
  }
  
  private class FilteredEdgeIterator<T> implements Iterator<Edge<T>> {
    private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Edge> reader;
    private String filter;
    
    public FilteredEdgeIterator(ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Edge> reader, String filter) {
      this.reader = reader;
      this.filter = filter;
    }
    
    @Override
    public boolean hasNext() {
      return reader.hasNext() && filter.equals(reader.peak().getFrom());
    }

    @Override
    public Edge<T> next() {
      org.andrewhitchcock.duwamish.protos.Duwamish.Edge edge = reader.next();
      T type = getTypeFromByteString(newEdgeBuilder, edge.getValue());
      return new Edge<T>(edge.getTo(), type);
    }

    @Override
    public void remove() {
      throw new RuntimeException("No!");
    }
  }
  
  private class FilteredMessageIterator<T> implements Iterator<T> {
    private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Message> reader;
    private String filter;
    
    public FilteredMessageIterator(ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Message> reader, String filter) {
      this.reader = reader;
      this.filter = filter;
    }
    
    @Override
    public boolean hasNext() {
      return reader.hasNext() && filter.equals(reader.peak().getDestination());
    }

    @Override
    public T next() {
      return getTypeFromByteString(newMessageBuilder, reader.next().getValue());
    }

    @Override
    public void remove() {
      throw new RuntimeException("No!");
    }
  }
}
