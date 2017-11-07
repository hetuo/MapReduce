package edu.usfca.cs.mr.travel;

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
public class TravelReducer1
        extends Reducer<Text, Text, Text, Text> {

    protected Map<String, Element> map = new HashMap<String, Element>();

    private class Element{
        double sum;
        int num;

        public Element(double sum, int num){
            this.sum = sum;
            this.num = num;
        }
    }

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String geohash = key.toString();
        for (Text value : values){
            String[] tokens = value.toString().split("\t");
            double tp = Double.parseDouble(tokens[1]);
            if (!map.containsKey(tokens[0]))
                map.put(tokens[0], new Element(tp, 1));
            else{
                map.get(tokens[0]).sum += tp;
                map.get(tokens[0]).num += 1;
            }
        }
        String month = null;
        double diff = 0.0;
        for (Map.Entry<String, Element> entry : map.entrySet()){
            if (month == null){
                month = entry.getKey();
                Element tmp = entry.getValue();
                diff = Math.abs(tmp.sum / tmp.num - 68);
            }else{
                Element tmp = entry.getValue();
                double diff1 = Math.abs(tmp.sum / tmp.num - 68);
                if (diff1 < diff){
                    month = entry.getKey();
                    diff = diff1;
                }
            }
        }

        if (month != null){
            Element tmp = map.get(month);
            double avg = tmp.sum / tmp.num;
            context.write(new Text(geohash), new Text(month + "\t" + Double.toString(avg)));
        }
    }

}