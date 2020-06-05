package com.kkb.mr02.customizepartitions;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class Main extends Configured implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("name","Customized Partitions");
        Job job = Job.getInstance(conf);

        job.setJarByClass(Main.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path(args[0]));

        job.setMapperClass(OrderMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(OrderBean.class);

        job.setCombinerClass(OrderCombiner.class);

        job.setReducerClass(OrderReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(OrderBean.class);

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        boolean b = job.waitForCompletion(true);

        return b?0:1;

    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        int run = ToolRunner.run(conf, new Main(), args);
        System.exit(run);
    }
}
