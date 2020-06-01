package com.kkb.mr.wordcountdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.InterruptedException;

public class MyReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> value, Context context) throws IOException, InterruptedException {
        int result = 0;
        for (IntWritable v: value) {
            result += v.get();
        }
        context.write(key,  new IntWritable(result));
    }

}
