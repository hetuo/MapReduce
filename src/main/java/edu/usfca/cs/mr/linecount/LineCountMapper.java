package edu.usfca.cs.mr.linecount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class LineCountMapper
        extends Mapper<LongWritable, Text, Text, IntWritable> {

    public Text newKey = new Text("Total lines");
    public final static IntWritable one = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        StringTokenizer itr = new StringTokenizer(value.toString());
        // emit word, count pairs.
        while (itr.hasMoreTokens()) {
            context.write(new Text(itr.nextToken()), new IntWritable(1));
        }

        context.write(newKey, one);
    }
}