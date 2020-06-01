package com.kkb.mr.customizedinputformatdemo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;

import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


// extends Configured implements Tool
public class Main extends Configured implements Tool {
    // Override run

    @Override
    public int run(String[] args) throws Exception {

        Configuration conf = super.getConf();
        Job job = Job.getInstance(conf,"Small files 2 sequencefile");
        job.setJarByClass(com.kkb.mr.customizedinputformatdemo.Main.class);
        //第一步：读取文件，解析成key,value对，k1:文件名 v1 文件的bytes
        job.setInputFormatClass(CustomizedInputFormat.class);
        CustomizedInputFormat.addInputPath(job,new Path(args[0]));

        //第二步：自定义map逻辑，接受k1   v1 转换成为新的k2   v2输出
        job.setMapperClass(CustomizedInputFormatMapper.class);

        //设置map阶段输出的key,value的类型，其实就是k2 v2的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        //第三步到六步：分区，排序，规约，分组都省略
        //第七步：自定义reduce逻辑,无reduce类
        //设置key3 value3的类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        //第八步：输出k3 v3 进行保存
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        SequenceFileOutputFormat.setOutputPath(job,new Path(args[1]));

        //一定要注意，输出路径是需要不存在的，如果存在就报错
        //提交job任务
        boolean b = job.waitForCompletion(true);
        return b?0:1;
    }


    // main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new Main(), args);
        System.exit(run);
    }



}
