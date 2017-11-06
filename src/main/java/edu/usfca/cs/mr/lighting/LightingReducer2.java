package edu.usfca.cs.mr.lighting;

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
public class LightingReducer2
        extends Reducer<Text, Text, Text, Text> {

    TreeMap<String, String> map = new TreeMap<>(new Comparator<String>() {
        @Override
        public int compare(String t1, String t2) {
            return t2.compareTo(t1);
        }
    });

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String lighting = null;
        for (Text value : values){
            if (lighting == null)
                lighting = value.toString();
            else if (value.toString().compareTo(lighting) > 0)
                lighting = value.toString();
        }
        String tmp = null;
        for (String l : map.keySet()){
            if (map.get(l).equals(key.toString()) && lighting.compareTo(l) > 0){
                tmp = l;
                break;
            }
        }
        if (tmp != null)
            map.remove(tmp);
        map.put(lighting, key.toString());
        if (map.size() > 3)
            map.remove(map.lastKey());
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        for (Map.Entry<String, String> entry : map.entrySet()){
            context.write(new Text(entry.getValue()), new Text(entry.getKey()));
        }
    }

}