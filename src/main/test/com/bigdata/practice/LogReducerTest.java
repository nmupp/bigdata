package com.bigdata.practice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by nmupp on 8/13/16.
 */
public class LogReducerTest {

    @Test
    public void processData() throws IOException {
        new ReduceDriver<Text, IntWritable, Text, IntWritable>()
                .withReducer(new LogReducer())
                .withInput(new Text("Narsi"), Arrays.asList(new IntWritable(2), new IntWritable(2)))
                .withOutput(new Text("Narsi"), new IntWritable(4))
                .runTest();
    }
}
