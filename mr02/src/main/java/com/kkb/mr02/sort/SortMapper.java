package com.kkb.mr02.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class SortMapper extends Mapper<LongWritable, Text, FlowBean, NullWritable> {
    private FlowBean flowBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        flowBean = new FlowBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        String line = value.toString();
        String[] fields =line.split("\t");

        this.flowBean.setPhone(fields[0]);
        this.flowBean.setUpFlow(Integer.parseInt(fields[1]));
        this.flowBean.setDownFlow(Integer.parseInt(fields[2]));
        this.flowBean.setUpCount(Integer.parseInt(fields[3]));
        this.flowBean.setDownCount(Integer.parseInt(fields[4]));

        context.write(flowBean, NullWritable.get());

    }
}
