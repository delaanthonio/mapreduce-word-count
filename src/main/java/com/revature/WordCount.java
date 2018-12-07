package com.revature;

import com.revature.map.AlphabetPartitioner;
import com.revature.map.WordCountMapper;
import com.revature.reduce.SumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: WordCount <input dir> <output dir>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "word_count");
    job.setJarByClass(WordCount.class);
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    FileInputFormat.setInputPaths(job, input);
    FileOutputFormat.setOutputPath(job, output);

    job.setMapperClass(WordCountMapper.class);
    job.setPartitionerClass(AlphabetPartitioner.class);
    job.setReducerClass(SumReducer.class);
    job.setNumReduceTasks(AlphabetPartitioner.MAX_PARTITIONS);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
