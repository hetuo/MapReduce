package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class SnowDepthMapper1
        extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        String[] tokens = value.toString().split("\\t");
        context.write(new Text(tokens[1]), new DoubleWritable(Double.parseDouble(tokens[50])));
    }
}