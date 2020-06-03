package com.kkb.mr02;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    private Text text;
    private FlowBean flowBean;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        text = new Text();
        flowBean = new FlowBean();
    }

    /*
    例子数据
    1363157985066	13726230503	00-FD-07-A4-72-B8:CMCC	120.196.100.82	i02.c.aliimg.com	游戏娱乐	24	27	2481	24681	200
    * */

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t" );
        String phoneNum = fields[1];
        String upFlow = fields[6];
        String downFlow = fields[7];
        String upCountFlow = fields[8];
        String downCountFlow = fields[9];

        text.set(phoneNum);
        flowBean.setUpFlow(Integer.parseInt(upFlow));
        flowBean.setDownFlow(Integer.parseInt(downFlow));
        flowBean.setUpCountFlow(Integer.parseInt(upCountFlow));
        flowBean.setDownCountFlow(Integer.parseInt(downCountFlow));

        context.write(text, flowBean);
    }
}
