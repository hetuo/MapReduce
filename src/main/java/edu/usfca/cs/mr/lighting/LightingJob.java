package edu.usfca.cs.mr.lighting;

import edu.usfca.cs.mr.linecount.LineCountJob;
import edu.usfca.cs.mr.linecount.LineCountMapper;
import edu.usfca.cs.mr.linecount.LineCountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by tuo on 05/11/17.
 */
public class LightingJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            Job job1 = Job.getInstance(conf, "lighting job1");
            job1.setJarByClass(LineCountJob.class);
            job1.setMapperClass(LineCountMapper.class);
            job1.setCombinerClass(LineCountReducer.class);
            job1.setReducerClass(LineCountReducer.class);
            job1.setMapOutputKeyClass(Text.class);
            job1.setMapOutputValueClass(Text.class);
            job1.setOutputKeyClass(Text.class);
            job1.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job1, new Path(args[0]));
            FileOutputFormat.setOutputPath(job1, new Path(args[1]));

            Job job2 = Job.getInstance(conf, "lighting job2");
            job2.setJarByClass(LineCountJob.class);
            job2.setMapperClass(LineCountMapper.class);
            job2.setCombinerClass(LineCountReducer.class);
            job2.setReducerClass(LineCountReducer.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit((job1.waitForCompletion(true) && job2.waitForCompletion(true))? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
