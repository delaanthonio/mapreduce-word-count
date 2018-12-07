package com.revature.map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** Partition words based on alphabet Requires 3 reducers */
public class AlphabetPartitioner extends Partitioner<Text, IntWritable> {

  public static final int MAX_PARTITIONS = 3;
  private static final char FIRST_SECTION_END = 'g';
  private static final char SECOND_SECTION_END = 'p';

  @Override
  public int getPartition(Text key, IntWritable value, int numReduceTasks) {
    char firstChar = key.toString().charAt(0);
    int partition;
    if (firstChar <= FIRST_SECTION_END) {
      partition = 0;
    } else if (firstChar <= SECOND_SECTION_END) {
      partition = 1;
    } else {
      partition = 2;
    }
    return partition % numReduceTasks;
  }
}
