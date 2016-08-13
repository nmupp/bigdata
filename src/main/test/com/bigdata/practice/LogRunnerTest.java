package com.bigdata.practice;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.hamcrest.core.Is;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by nmupp on 8/13/16.
 */
public class LogRunnerTest {

    @Test
    public void testRunner() throws Exception {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","file:///");
        conf.set("mapreduce.framework.name","local");
        conf.setInt("mapreduce.task.io.sort.mb",1);

        Path input = new Path("input");
        Path output = new Path("output");

        FileSystem fs = FileSystem.getLocal(conf);
        fs.delete(output, true);

        LogRunner runner = new LogRunner();
        runner.setConf(conf);

        int exitCode = runner.run(new String[] {
                input.toString(),output.toString()
        });
        Assert.assertThat(exitCode, Is.is(0));
    }
}
