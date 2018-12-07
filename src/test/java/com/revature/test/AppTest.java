package com.revature.test;

import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;
import com.revature.map.WordCountMapper;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import java.util.Arrays;
import java.util.List;
import com.revature.reduce.SumReducer;


public class AppTest {
    private MapDriver<LongWritable, Text, Text, IntWritable> mapDriver;
    private ReduceDriver<Text, IntWritable, Text, IntWritable> reduceDriver;
    private MapReduceDriver<LongWritable, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

    @Before
    public void setUp() {
        WordCountMapper mapper = new WordCountMapper();
        mapDriver = new MapDriver<>();
        mapDriver.setMapper(mapper);

        SumReducer reducer = new SumReducer();
        reduceDriver = new ReduceDriver<>();
        reduceDriver.setReducer(reducer);

        mapReduceDriver = new MapReduceDriver<>();
        mapReduceDriver.setMapper(mapper);
        mapReduceDriver.setReducer(reducer);
    }

    @Test
    public void testWordCountMapper() {
        // Input
        mapDriver.withInput(new LongWritable(1), new Text("cat cat dog"));

        // Expected output
        mapDriver.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver.withOutput(new Text("cat"), new IntWritable(1));
        mapDriver.withOutput(new Text("dog"), new IntWritable(1));
        mapDriver.runTest();
    }

    @Test
    public void testWordSumReduce() {
        List<IntWritable> values = Arrays.asList(new IntWritable(1), new IntWritable(1)); 

        reduceDriver.withInput(new Text("cat"), values);
        reduceDriver.withOutput(new Text("cat"), new IntWritable(2));
        reduceDriver.runTest();
    }

    @Test
    public void testWordCountMapReduce() {
        mapReduceDriver.withInput(new LongWritable(1), new Text("cat cat dog"));
        mapReduceDriver.withOutput(new Text("cat"), new IntWritable(2));
        mapReduceDriver.withOutput(new Text("dog"), new IntWritable(1));
        mapReduceDriver.runTest();
    }
}
