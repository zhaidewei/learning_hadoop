package com.hello;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class HdfsOperationsByJava {

    public static void main(String[] args) throws IOException {
       mkDir();
       copyFromLocal(args);
    }

    static void mkDir() throws IOException {
        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fs = FileSystem.get(configuration);
        fs.mkdirs(new Path("/kkb/dir1"));
        fs.close();
    }

    static void copyFromLocal(String[] args) throws IOException {

        Configuration configuration = new Configuration();
        configuration.set("fs.defaultFS", "hdfs://node01:8020");
        FileSystem fs = FileSystem.get(configuration);
        fs.copyFromLocalFile(
                new Path(args[0]),
                new Path(args[1]));
        fs.close();
    }
}
