package edu.usfca.cs.mr.hottest;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.StringTokenizer;

/**
 * Mapper: Reads line by line, split them into words. Emit <word, 1> pairs.
 */
public class HottestMapper1
        extends Mapper<LongWritable, Text, Text, Text> {



    protected String getMonth(String timestamp){
        SimpleDateFormat t = new SimpleDateFormat("yyyy-MM");
        long s = Long.parseLong(timestamp);
        return t.format(s);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\t");
        String geohash = tokens[1];
        String temp = getMonth(tokens[0]) + "\t" + tokens[40];
        context.write(new Text(geohash), new Text(temp));
    }
}