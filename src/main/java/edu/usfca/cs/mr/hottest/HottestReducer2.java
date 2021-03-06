package edu.usfca.cs.mr.hottest;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;



/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class HottestReducer2
        extends Reducer<Text, Text, Text, Text> {
    String geohash = null;
    String time = null;
    String temperature = null;
    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values){
            String[] tmp = value.toString().split("\\t");
            if (geohash == null && time == null && temperature == null){
                geohash = tmp[0];
                time = tmp[1];
                temperature = tmp[2];
            }else if (tmp[1].compareTo(temperature) > 0){
                geohash = tmp[0];
                time = tmp[1];
                temperature = tmp[2];
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        if (geohash != null)
            context.write(new Text("Hottest"), new Text(geohash + "\t" + time + "\t" + temperature));
    }

}