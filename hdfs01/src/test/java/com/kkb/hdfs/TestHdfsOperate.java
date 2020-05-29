package com.kkb.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

public class TestHdfsOperate {

    static String nameNodeURL =  "hdfs://node01:8020";
    static Configuration configuration = new Configuration();

    static FileSystem getFilesystem () throws IOException {
        configuration.set("fs.defaultFS", nameNodeURL);
        return FileSystem.get(configuration);
    }

    @Test
    public void mkdir() throws IOException {
        FileSystem fs = TestHdfsOperate.getFilesystem();
        fs.mkdirs(new Path("/kkb/dir1"));
        fs.close();
    }

    @Test
    public void putLocalFile() throws IOException {
        FileSystem fs = TestHdfsOperate.getFilesystem();

        String currentDir = new File("").getAbsolutePath();
        fs.copyFromLocalFile(
                new Path("file://" + currentDir + "/pom.xml"),
                new Path(TestHdfsOperate.nameNodeURL + "/kkb/dir1/pom.xml"));
        fs.close();
    }
}
