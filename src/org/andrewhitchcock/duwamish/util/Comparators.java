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
