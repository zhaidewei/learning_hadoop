package com.kkb.mr02.customizepartitions;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class OrderMapper extends Mapper<LongWritable, Text, Text, OrderBean> {
    private OrderBean orderBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        orderBean = new OrderBean();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");

        Text outKey = new Text();

        outKey.set(words[0]);
        orderBean.setProductid(words[1]);
        orderBean.setPrice(Float.parseFloat(words[2]));


        context.write(outKey, orderBean);
    }
}
