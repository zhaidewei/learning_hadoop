package com.kkb.mr.customizedinputformatdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class CustomizedRecordReader extends RecordReader<NullWritable, BytesWritable> {

    //跟踪变量放在这里声明，只声明不赋值。
    private BytesWritable byteswritable;
    private Boolean flag;
    private FileSplit fileSplit;
    private Configuration configuration;


    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {

        // split
        fileSplit = (FileSplit) inputSplit;

        // Configuration
        configuration = taskAttemptContext.getConfiguration();

        // BytesWritable初始化
        byteswritable = new BytesWritable();

        // Flag，该file split是否已经被读取过了？
        flag = false;
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        /*
        FileSystem.open() -> in
        in -> bytes[] -> 输入的value
         */
        if (!flag) {
            //需要知道大小才能声明一个byte数组
            int length = (int) fileSplit.getLength();
            byte[] bytes = new byte[length];

            //怎样获得文件系统？
            // fileSplit -> path -> fs
            Path path = fileSplit.getPath();
            FileSystem fileSystem = path.getFileSystem(configuration);

            FSDataInputStream in = fileSystem.open(path);//这个操作告诉我了fileSplit的本质只是一个metadata集合，并非已经读取的数据。

            //in -> bytes[]

            IOUtils.readFully(in, bytes,0, length);

            // bytes 转换为 byteswritable
            byteswritable.set(bytes, 0, length);

            flag = true;
            return true;
        }
        return false;
    }

    @Override
    public NullWritable getCurrentKey() throws IOException, InterruptedException {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return byteswritable;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return flag?1:0;
    }

    @Override
    public void close() throws IOException {

    }

}
