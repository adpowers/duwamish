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

package org.andrewhitchcock.duwamish.example;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class BenchmarkSuite {

  @SuppressWarnings("unchecked")
  public static void main(String[] args) throws Exception {
    Map<Class, ArrayList<ArrayList<String>>> tests = Maps.newHashMap();

    tests.put(PageRank.class, Lists.newArrayList(
        Lists.newArrayList("5000", "1"),
        Lists.newArrayList("20000", "1"),
        Lists.newArrayList("100000", "1"),
        Lists.newArrayList("250000", "1")));
    
    tests.put(Recommendations.class, Lists.newArrayList(
        Lists.newArrayList("1000", "10000", "64", "1"),
        Lists.newArrayList("5000", "50000", "64", "1")));
        //Lists.newArrayList("10000", "100000", "64", "1")));
    
    tests.put(ShortestPath.class, Lists.newArrayList(
        Lists.newArrayList("5000", "1"),
        Lists.newArrayList("20000", "1"),
        Lists.newArrayList("100000", "1"),
        Lists.newArrayList("250000", "1"),
        Lists.newArrayList("500000", "1")));

    for (Map.Entry<Class, ArrayList<ArrayList<String>>> entry : tests.entrySet()) {
      Class clazz = entry.getKey();
      for (ArrayList<String> arguments : entry.getValue()) {
        bestOfThree(clazz, arguments);
      }
    }
  }

  @SuppressWarnings("unchecked")
  private static void bestOfThree(Class clazz, List<String> args) throws Exception{
    Method mainMethod = clazz.getMethod("main", String[].class);
    Object[] passedArgs = { args.toArray(new String[0]) };
    
    long[] times = new long[3];
    for (int i = 0; i < 3; i++) {
      long startTime = System.currentTimeMillis();
      mainMethod.invoke(null, passedArgs);
      long endTime = System.currentTimeMillis();
      times[i] = endTime - startTime;
      System.gc();
    }
    
    long bestTime = Math.min(Math.min(times[0], times[1]), times[2]);
    System.out.println(Joiner.on(" ").join(clazz.getCanonicalName(), Joiner.on(", ").join(args), "bestTime", bestTime));
  }
}
