package org.andrewhitchcock.duwamish.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Deque;
import java.util.List;

import com.google.common.collect.Lists;
import com.google.common.io.FileBackedOutputStream;
import com.google.protobuf.Message;


public class MergeSorter<T extends Message> {
  
  final int recordsToSortAtOnce = 1000;
  final int numberToMergeInMemory = 20;
  final int numberToMergeFromDisk = 50;

  final Class<T> clazz;
  @SuppressWarnings("unchecked")
  final Comparator comparator;
  
  int sortCount = 0;
  Deque<FileBackedOutputStream> mergeQueue = new ArrayDeque<FileBackedOutputStream>();
  
  final Method builderMethod;
  
  private MergeSorter(Class<T> clazz, Comparator<T> comparator) {
    this.clazz = clazz;
    this.comparator = comparator;
    try {
      this.builderMethod = clazz.getMethod("newBuilder");
    } catch (Exception e) {
      throw new RuntimeException();
    }
  }
  
  public static <T extends Message> MergeSorter<T> create(Class<T> clazz, Comparator<T> comparator) {
    return new MergeSorter<T>(clazz, comparator);
  }
  
  
  public void sort(File outputFile, File ... inputFiles) {
    // sort each input file
    for (File inputFile : inputFiles) {
      sortOneFile(inputFile);
    }
    
    // merge files
    try {
      while (!mergeQueue.isEmpty()) {
        boolean moreThanOnePassLeft = mergeQueue.size() > numberToMergeFromDisk;
        int currentPassSize = moreThanOnePassLeft ? numberToMergeFromDisk : mergeQueue.size();
        
        InputStream[] inputs = new InputStream[currentPassSize];
        for (int i = 0; i < currentPassSize; i++) {
          inputs[i] = new BufferedInputStream(mergeQueue.pop().getSupplier().getInput());
        }
        
        OutputStream output = moreThanOnePassLeft ? getNextOutputStream() : FileUtil.newOutputStream(outputFile);
        mergeInputStreams(output, inputs);
        
        FileUtil.closeAll(inputs);
        if (moreThanOnePassLeft) {
          ((BufferedOutputStream) output).flush();
        } else {
          output.close();
        }
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  @SuppressWarnings("unchecked")
  private void sortOneFile(File inputFile) {
    ProtocolBufferReader<T> inputReader = ProtocolBufferReader.newReader(clazz, FileUtil.newInputStream(inputFile));

    List<Object[]> inMemorySortedArrays = Lists.newArrayList();
    Object[] records = new Object[recordsToSortAtOnce];
    
    int pos = 0;
    while (inputReader.hasNext()) {
      if (pos == recordsToSortAtOnce) {
        Arrays.sort(records, 0, pos, comparator);
        inMemorySortedArrays.add(records);
        records = new Object[recordsToSortAtOnce];
        pos = 0;
      }
      if (inMemorySortedArrays.size() > numberToMergeInMemory) {
        mergeInMemorySortedArrays(getNextOutputStream(), inMemorySortedArrays);
      }
      
      records[pos] = inputReader.next();
      pos++;
    }
    
    if (pos != 0) {
      Arrays.sort(records, 0, pos, comparator);
      inMemorySortedArrays.add(records);
    }
    if (!inMemorySortedArrays.isEmpty()) {
      mergeInMemorySortedArrays(getNextOutputStream(), inMemorySortedArrays);
    }
    
    FileUtil.closeAll(inputReader);
  }
  
  @SuppressWarnings("unchecked")
  private void mergeInMemorySortedArrays(BufferedOutputStream outputStream, List<Object[]> inMemorySortedArrays) {
    ProtocolBufferReader<T>[] entries = new ProtocolBufferReader[inMemorySortedArrays.size()];
    for (int i = 0; i < entries.length; i++) {
      entries[i] = ProtocolBufferReader.newReader(inMemorySortedArrays.get(i));
    }
    inMemorySortedArrays.clear();
    
    merge(outputStream, entries);
    
    try {
      outputStream.flush();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  private void merge(OutputStream outputStream, ProtocolBufferReader<T>[] entries) {
    try {
      int bestSoFar = returnLowestIndex(entries);
      while (bestSoFar != -1) {
        entries[bestSoFar].next().writeDelimitedTo(outputStream);
        bestSoFar = returnLowestIndex(entries);
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
  
  @SuppressWarnings("unchecked")
  private int returnLowestIndex(ProtocolBufferReader<T>[] readers) {
    int bestIndexSoFar = -1;
    T bestSoFar = null;
    for (int i = 0; i < readers.length; i++) {
      ProtocolBufferReader<T> reader = readers[i];
      if (reader.hasNext()) {
        if (bestSoFar == null || comparator.compare(bestSoFar, reader.peak()) > 0) {
          bestIndexSoFar = i;
          bestSoFar = reader.peak();
        }
      }
    }
    return bestIndexSoFar;
  }
  
  private void mergeInputStreams(OutputStream outputStream, InputStream ... inputStreams) {
    @SuppressWarnings("unchecked")
    ProtocolBufferReader<T>[] entries = new ProtocolBufferReader[inputStreams.length];
    for (int i = 0; i < inputStreams.length; i++) {
      entries[i] = ProtocolBufferReader.newReader(clazz, inputStreams[i]);
    }

    merge(outputStream, entries);
  }
  
  private BufferedOutputStream getNextOutputStream() {
    FileBackedOutputStream temp = new FileBackedOutputStream(1024 * 1024); // 1 MB
    mergeQueue.addLast(temp);
    return new BufferedOutputStream(temp);
  }
}
