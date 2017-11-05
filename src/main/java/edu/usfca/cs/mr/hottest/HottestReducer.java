package edu.usfca.cs.mr.hottest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class HottestReducer
        extends Reducer<Text, Text, Text, Text> {
    String geohash = null;
    String time = null;
    String temperature = null;
    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values){
            String[] tmp = value.toString().split(",");
            if (geohash == null && time == null && temperature == null){
                geohash = key.toString();
                time = tmp[0];
                temperature = tmp[1];
            }else if (tmp[1].compareTo(temperature) > 0){
                geohash = key.toString();
                time = tmp[0];
                temperature = tmp[1];
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        context.write(new Text(geohash), new Text(time + " " + temperature));
    }

}