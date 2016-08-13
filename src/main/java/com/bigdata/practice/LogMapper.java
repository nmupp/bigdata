package com.bigdata.practice;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by nmupp on 8/11/16.
 */
public class LogMapper extends Mapper<LongWritable,Text,Text,IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String [] data = value.toString().split(",");
        for(String dataValue:data) {
            if(dataValue.equals("Narsi")) {
                context.write(new Text(dataValue),new IntWritable(1));
            }
        }
    }
}
