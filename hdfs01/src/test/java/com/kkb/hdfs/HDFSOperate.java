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
        String nameNodeDns = System.getenv("NAMENODE_DNS");
        configuration.set("fs.defaultFS", "hdfs://" + nameNodeDns + ":8020");

        FileSystem fs = FileSystem.get(configuration);

        fs.mkdirs(new Path("/kaikeba/dir1"));

        fs.close();

    }
}
