package com.bigdata.practice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.junit.Test;

import java.io.IOException;

/**
 * Created by nmupp on 8/13/16.
 */
public class LogMapperTest {

    @Test
    public void processData() throws IOException {
        Text value = new Text("Narsi,bunnu,Narsi,Narsi");
        new MapDriver<LongWritable,Text,Text,IntWritable>()
                .withMapper(new LogMapper())
                .withInput(new LongWritable(0),value)
                .withOutput(new Text("Narsi"),new IntWritable(3))
                .runTest();
    }

}
