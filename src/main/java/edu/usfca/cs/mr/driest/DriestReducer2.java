package edu.usfca.cs.mr.driest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.PriorityQueue;


/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class DriestReducer2
        extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values){
            String nextKey = value.toString().substring(0, 7);
            String nextValue = value.toString().substring(8);
            context.write(new Text(nextKey), new Text(nextValue));
        }
    }

}