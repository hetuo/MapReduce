package edu.usfca.cs.mr.driest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by tuo on 05/11/17.
 */
public class DriestMapper2
        extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        //String[] tokens = value.toString().split("\\t");

        context.write(new Text("Bay Area"), value);
    }
}
