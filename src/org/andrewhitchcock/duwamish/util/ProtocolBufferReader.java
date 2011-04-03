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

import java.io.Closeable;
import java.io.InputStream;
import java.lang.reflect.Method;
import java.util.Iterator;

import com.google.protobuf.Message.Builder;

public abstract class ProtocolBufferReader<T> implements Iterator<T>, Closeable {
  
  protected boolean initialized = false;
  protected boolean success;
  protected T nextValue;
  
  protected abstract void getNext();

  @Override
  public boolean hasNext() {
    initialize();
    return success;
  }

  @Override
  public T next() {
    initialize();
    T result = nextValue;
    getNext();
    return result;
  }

  @Override
  public void remove() {
    throw new RuntimeException("Not implemented");
  }
  
  public T peak() {
    initialize();
    return nextValue;
  }
  
  private void initialize() {
    if (!initialized) {
      getNext();
      initialized = true;
    }
  }
  
  
  
  public static <A> ProtocolBufferReader<A> newReader(Class<A> clazz, final InputStream inputSteam) {
    Method bm;
    try {
      bm = clazz.getMethod("newBuilder");
    } catch (Exception e) {
      throw new RuntimeException();
    }
    final Method finalBm = bm;
    
    return new ProtocolBufferReader<A>() {
      private InputStream is = inputSteam;
      private Method builderMethod = finalBm;
      
      @SuppressWarnings("unchecked")
      @Override
      protected void getNext() {
        try {
          Builder builder = (Builder)(builderMethod.invoke(null));
          success = builder.mergeDelimitedFrom(is);
          if (success) {
            nextValue = (A) builder.build();
          } else {
            nextValue = null;
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public void close() {
        FileUtil.closeAll(is);
      }
    };
  }
  
  public static <A> ProtocolBufferReader<A> newReader(final Object[] values) {
    return new ProtocolBufferReader<A>() {
      private Object[] objects = values;
      private int index = 0;
      
      @SuppressWarnings("unchecked")
      @Override
      protected void getNext() {
        if (index < objects.length && objects[index] != null) {
          nextValue = (A) objects[index];
          success = true;
          index++;
        } else {
          nextValue = null;
          success = false;
        }
      }
      
      @Override
      public void close() {
        // no op
      }
    };
  }
}
