package edu.usfca.cs.mr.green;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class GreenMapper2
        extends Mapper<LongWritable, Text, Text, Text> {

    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        context.write(new Text("best"), value);
    }
}
