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

import org.andrewhitchcock.duwamish.model.Edge;

import com.google.common.collect.Multimap;
import com.google.protobuf.Message;

@SuppressWarnings("unchecked")
public class Context<V extends Message, E extends Message, M extends Message> {
  private long superstepNumber;
  private Iterable<Edge<E>> edgeIterable;
  private Partition partition;
  private Multimap<String, Object> accumulationMessages;
  private boolean votedToHalt = false;
  
  public Context(long superstepNumber, Partition partition, Multimap<String, Object> accumulationMessages) {
    this.superstepNumber = superstepNumber;
    this.partition = partition;
    this.accumulationMessages = accumulationMessages;
  }
  
  public void setEdgeIterable(Iterable<Edge<E>> edgeIterable) {
    this.edgeIterable = edgeIterable;
  }
  
  public long getSuperstepNumber() {
    return superstepNumber;
  }
  
  public Iterable<Edge<E>> getEdgeIterable() {
    return edgeIterable;
  }
  
  public void sendMessageTo(String vertexId, M message) {
    partition.sendMessage(vertexId, message);
  }
  
  public void voteToHalt() {
    votedToHalt = true;
  }
  
  public boolean getVotedToHalt() {
    return votedToHalt;
  }
  
  public void emitAccumulation(String name, Object value) {
    accumulationMessages.put(name, value);
  }
}
