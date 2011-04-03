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

package org.andrewhitchcock.duwamish.util;

import java.util.Comparator;

import org.andrewhitchcock.duwamish.protos.Duwamish.Edge;
import org.andrewhitchcock.duwamish.protos.Duwamish.Message;
import org.andrewhitchcock.duwamish.protos.Duwamish.Vertex;

public class Comparators {
  public static Comparator<Vertex> newVertexComparator() {
    return new Comparator<Vertex>() {
      @Override
      public int compare(Vertex o1, Vertex o2) {
        return o1.getId().compareTo(o2.getId());
      }
    };
  }
  
  public static Comparator<Edge> newEdgeComparator() {
    return new Comparator<Edge>() {
      @Override
      public int compare(Edge o1, Edge o2) {
        return o1.getFrom().compareTo(o2.getFrom());
      }
    };
  }
  
  public static Comparator<Message> newMessageComparator() {
    return new Comparator<Message>() {
      @Override
      public int compare(Message o1, Message o2) {
        return o1.getDestination().compareTo(o2.getDestination());
      }
    };
  }
}
