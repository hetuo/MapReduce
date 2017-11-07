package edu.usfca.cs.mr.climate;

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
public class ClimateReducer2
        extends Reducer<Text, Text, Text, Text> {


    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values){
            //System.out.println(value.toString());
            if (key.toString().equals("Bay Area")){
                String str = value.toString();
                String nextKey = str.substring(0, 2);
                String nextValue = str.substring(3);
                context.write(new Text(nextKey), new Text(nextValue));
            } else{
                context.write(key, value);
            }
        }
    }

}