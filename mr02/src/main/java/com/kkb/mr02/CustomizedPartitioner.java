package com.kkb.mr02;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class CustomizedPartitioner extends HashPartitioner<Text, FlowBean> {

    @Override
    public int getPartition(Text key, FlowBean value, int numReduceTasks) {
        String phoneNumber = key.toString();
        if (phoneNumber.equals(null) || phoneNumber.equals("")) {
            return 5;
        }
        switch (phoneNumber.substring(0,3)) {
            case "135": return 0;
            case "136": return 1;
            case "137": return 2;
            case "138": return 3;
            case "139": return 4;
            default: return 5;

        }

    }
}
