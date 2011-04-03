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

import java.util.List;

import org.andrewhitchcock.duwamish.Partition;
import org.andrewhitchcock.duwamish.model.Partitioner;

@SuppressWarnings("unchecked")
public class HashPartitioner extends Partitioner {
  private List<Partition> partitions;
  
  public HashPartitioner(List<Partition> partitions) { 
    this.partitions = partitions;
  }
  
  @Override
  public int getPartitionIdByVertex(String vertexId) {
    return Math.abs(vertexId.hashCode()) % partitions.size();
  }
}
