package edu.usfca.cs.mr.lighting;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.*;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class LightingReducer1
        extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double lighting = 0.0;
        int num = 0;
        for (Text value : values){
            lighting += Double.parseDouble(value.toString());
            num++;
        }
        if (num != 0)
            context.write(key, new Text(Double.toString(lighting / num)));
    }

}