package org.andrewhitchcock.duwamish.model;

import org.andrewhitchcock.duwamish.Context;

import com.google.protobuf.Message;

public abstract class Vertex<V extends Message, E extends Message, M extends Message> {
  public abstract V compute(String vertexId, V value, Iterable<M> messages, Context<V, E, M> context);
}