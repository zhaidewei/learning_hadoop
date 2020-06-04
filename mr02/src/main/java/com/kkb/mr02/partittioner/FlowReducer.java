package com.kkb.mr02.partittioner;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 对四个字段进行累加
        int upFlow = 0;
        int downFlow = 0;
        int upCountFlow = 0;
        int downCountFlow = 0;

        for(FlowBean v: values) {
            upFlow += v.getUpFlow();
            downFlow += v.getDownFlow();
            upCountFlow += v.getUpCountFlow();
            downCountFlow += v.getDownCountFlow();
        }

        Text text = new Text(
                "{"
                + "upflow=" + upFlow + ","
                + "downflow=" + downFlow + ","
                + "upcountflow=" + upCountFlow + ','
                + "downcountflow=" + downCountFlow
                + "}");

        context.write(key, text);
    }
}
