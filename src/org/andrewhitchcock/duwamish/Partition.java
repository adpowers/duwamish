/**
 * Copyright 2011 Andrew Hitchcock
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.andrewhitchcock.duwamish;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
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
import com.google.common.collect.Multimap;
import com.google.common.io.FileBackedOutputStream;
import com.google.protobuf.ByteString;
import com.google.protobuf.Message;
import com.google.protobuf.Message.Builder;

public class Partition<C extends Vertex<V, E, M>, V extends Message, E extends Message, M extends Message> {
  
  private final int incomingMessageThreshold = 1024 * 1024;
  
  @SuppressWarnings("unchecked")
  private Map<String, Accumulator> accumulators;
  private Partitioner partitioner;
  
  private final C vertex;
  private final Method newVertexBuilder;
  private final Method newEdgeBuilder;
  private final Method newMessageBuilder;
  
  @SuppressWarnings("unchecked")
  private Partition[] partitions;

  private final File edgeFile;
  private final File vertexFile;
  
  private final File newVertexFile;
  
  private OutputStream edgeFileWriter;
  private OutputStream vertexFileWriter;
  
  private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Vertex> vertexReader;
  private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Edge> edgeReader;
  private ProtocolBufferReader<org.andrewhitchcock.duwamish.protos.Duwamish.Message> messageReader;
  
  private FileBackedOutputStream incomingMessages;
  private BufferedOutputStream[] messageWriters;
  
  @SuppressWarnings("unchecked")
  public Partition(DuwamishContext<C, V, E, M> duwamishContext, Map<String, Accumulator> accumulators, File tempDir, Partition[] partitions, int partitionNumber) {
    this.accumulators = accumulators;
    this.partitions = partitions;
    this.edgeFile = new File(tempDir, "edge-" + partitionNumber);
    this.vertexFile = new File(tempDir, "vertex-" + partitionNumber);
    this.newVertexFile = new File(tempDir, "new-vertex-" + partitionNumber);
    
    this.edgeFileWriter = FileUtil.newOutputStream(edgeFile);
    this.vertexFileWriter = FileUtil.newOutputStream(vertexFile);
    
    this.incomingMessages = new FileBackedOutputStream(incomingMessageThreshold);
    
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
    
    MergeSorter.create(org.andrewhitchcock.duwamish.protos.Duwamish.Edge.class, Comparators.newEdgeComparator()).sort(edgeFile, tempEdgeFile);
    MergeSorter.create(org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.class, Comparators.newVertexComparator()).sort(vertexFile, tempVertexFile);
    
    tempEdgeFile.delete();
    tempVertexFile.delete();
  }
  
  public void sendMessage(String vertexId, M message) {
    try {
      int targetPartitionId = partitioner.getPartitionIdByVertex(vertexId);
      org.andrewhitchcock.duwamish.protos.Duwamish.Message.newBuilder()
        .setDestination(vertexId)
        .setValue(message.toByteString())
        .build().writeDelimitedTo(messageWriters[targetPartitionId]);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public void setup(Partitioner partitioner) {
    this.partitioner = partitioner;
  }
  
  public void before() {
    try {
      // Sort incoming messages
      FileBackedOutputStream messages = new FileBackedOutputStream(incomingMessageThreshold);
  
      MergeSorter.create(
          org.andrewhitchcock.duwamish.protos.Duwamish.Message.class,
          Comparators.newMessageComparator())
          .sort(messages, new BufferedInputStream(incomingMessages.getSupplier().getInput()));
      
      incomingMessages = new FileBackedOutputStream(incomingMessageThreshold);
      
      // Open readers
      vertexReader = ProtocolBufferReader.newReader(org.andrewhitchcock.duwamish.protos.Duwamish.Vertex.class, FileUtil.newInputStream(vertexFile));
      edgeReader = ProtocolBufferReader.newReader(org.andrewhitchcock.duwamish.protos.Duwamish.Edge.class, FileUtil.newInputStream(edgeFile));
      messageReader = ProtocolBufferReader.newReader(org.andrewhitchcock.duwamish.protos.Duwamish.Message.class, new BufferedInputStream(messages.getSupplier().getInput()));
      
      // Open new vertex value writer
      vertexFileWriter = FileUtil.newOutputStream(newVertexFile);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  public Map<String, Object> runSuperstep(long superstepNumber) {
    // Open outgoing message writers
    messageWriters = new BufferedOutputStream[partitions.length];
    for (int i = 0; i < messageWriters.length; i++) {
      messageWriters[i] = new BufferedOutputStream(partitions[i].incomingMessages);
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
    
    for (int i = 0; i < messageWriters.length; i++) {
      try {
        messageWriters[i].flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
    messageWriters = null;
    
    vertexReader = null;
    edgeReader = null;
    messageReader = null;
    vertexFileWriter = null;
    
    if (vertexFile.exists()) {
      vertexFile.delete();
    }
    
    newVertexFile.renameTo(vertexFile);
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
