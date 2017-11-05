package edu.usfca.cs.mr.snowdepth;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Reducer: Input to the reducer is the output from the mapper. It receives
 * word, list<count> pairs.  Sums up individual counts per given word. Emits
 * <word, total count> pairs.
 */
public class SnowDepthReducer
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    TreeMap<Double, String> map = new TreeMap<>(new Comparator<Double>() {
        @Override
        public int compare(Double t1, Double t2) {
            return t2.compareTo(t1);
        }
    });

    @Override
    protected void reduce(
            Text key, Iterable<DoubleWritable> values, Context context)
            throws IOException, InterruptedException {
        double sum = 0;
        for (DoubleWritable value : values){
            if (value.get() <= 0.0)
                return;
            sum += value.get();
        }
        double tmp = 0.0;
        for (Double d : map.keySet()){
            if (map.get(d).equals(key.toString()) && sum > d){
                tmp = d;
                break;
            }
        }
        if (tmp != 0.0)
            map.remove(tmp);
        map.put(sum, key.toString());
        if (map.size() > 1)
            map.remove(map.lastKey());
    }

    @Override
    protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
        throws IOException, InterruptedException {
        for (Map.Entry<Double, String> entry : map.entrySet()){
            context.write(new Text(entry.getValue()), new DoubleWritable(entry.getKey()));
        }
    }

}