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
