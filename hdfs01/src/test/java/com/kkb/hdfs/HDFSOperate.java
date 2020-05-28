package com.kkb.hdfs;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;

public class HDFSOperate {
    @Test
    public void mkdir() throws IOException {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://ec2-34-240-61-244.eu-west-1.compute.amazonaws.com:8020");
//        configuration.set("")

        FileSystem fs = FileSystem.get(configuration);

        fs.mkdirs(new Path("/kaikeba/dir1"));

        fs.close();

    }
}
