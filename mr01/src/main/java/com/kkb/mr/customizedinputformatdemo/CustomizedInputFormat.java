package com.kkb.mr.customizedinputformatdemo;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

public class CustomizedInputFormat extends FileInputFormat<NullWritable, BytesWritable> {
    @Override
    public RecordReader<NullWritable, BytesWritable> createRecordReader(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        CustomizedRecordReader customizedRecordReader = new CustomizedRecordReader();
        customizedRecordReader.initialize(inputSplit, taskAttemptContext);
        return customizedRecordReader;

    }

    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
}
