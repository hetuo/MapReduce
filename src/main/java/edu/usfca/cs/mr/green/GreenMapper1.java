package edu.usfca.cs.mr.green;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class GreenMapper1
        extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        String[] tokens = value.toString().split("\\t");
        if (tokens[18].toString().equals("1.0"))
            context.write(new Text(tokens[1].substring(0,4)), new Text(tokens[16] + "\t" + tokens[15]));
    }
}
