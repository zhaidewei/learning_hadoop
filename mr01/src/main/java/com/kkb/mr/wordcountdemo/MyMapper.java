package com.kkb.mr.wordcountdemo;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 自定义mapper类需要继承Mapper，有四个泛型，
 * keyin: k1   行偏移量 Long
 * valuein: v1   一行文本内容   String
 * keyout: k2   每一个单词   String
 * valueout : v2   1         int
 * 在hadoop当中没有沿用Java的一些基本类型，使用自己封装了一套基本类型
 * long ==>LongWritable
 * String ==> Text
 * int ==> IntWritable
 *
 */
public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = key.toString();
        String[] words = line.split(",");

        Text outKey = new Text();
        IntWritable outValue = new IntWritable(1);

        for (String word: words) {
            outKey.set(word);
            context.write(outKey, outValue);
        }
    }

}
