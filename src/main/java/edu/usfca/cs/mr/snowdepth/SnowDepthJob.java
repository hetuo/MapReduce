package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * Created by tuo on 04/11/17.
 */
public class SnowDepthJob {
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn
            // webapp.
            Job job = Job.getInstance(conf, "snow depth job1");
            // Current class.
            job.setJarByClass(SnowDepthJob.class);
            // Mapper
            job.setMapperClass(SnowDepthMapper1.class);
            // Combiner. We use the reducer as the combiner in this case.
            job.setCombinerClass(SnowDepthReducer1.class);
            // Reducer
            job.setReducerClass(SnowDepthReducer1.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            //job.setMapOutputValueClass(IntWritable.class);
            job.setMapOutputValueClass(DoubleWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following
            // two properties if the Mapper and Reducer has same key and value
            // types. It is set separately for elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(DoubleWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path(args[0]));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
            // Block until the job is completed.


            Job job2 = Job.getInstance(conf, "snow depth job2");
            job2.setJarByClass(SnowDepthJob.class);
            job2.setMapperClass(SnowDepthMapper2.class);
            job2.setCombinerClass(SnowDepthReducer2.class);
            job2.setReducerClass(SnowDepthReducer2.class);
            job2.setMapOutputKeyClass(Text.class);
            job2.setMapOutputValueClass(Text.class);
            job2.setOutputKeyClass(Text.class);
            job2.setOutputValueClass(Text.class);
            FileInputFormat.addInputPath(job2, new Path(args[1]));
            FileOutputFormat.setOutputPath(job2, new Path(args[2]));

            System.exit(job.waitForCompletion(true) && job2.waitForCompletion(true)? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }
    }
}
