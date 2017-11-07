package edu.usfca.cs.mr.snowdepth;

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
public class SnowDepthReducer1
        extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {


    PriorityQueue<Element> queue = new PriorityQueue<Element>(10, new Comparator<Element>() {
        @Override
        public int compare(Element e1, Element e2) {
            if (e1.snow > e2.snow)
                return 1;
            else if (e1.snow < e2.snow)
                return -1;
            else
                return 0;
        }
    });


    private class Element{
        public String geohash;
        public double snow;

        public Element(String geohash, double snow){
            this.geohash = geohash;
            this.snow = snow;
        }
    }


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
        if (queue.size() < 3)
            queue.add(new Element(key.toString(), sum));
        else{
            Element tmp = queue.peek();
            if (sum > tmp.snow){
                queue.poll();
                queue.add(new Element(key.toString(), sum));
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, DoubleWritable, Text, DoubleWritable>.Context context)
        throws IOException, InterruptedException {
        Iterator<Element> iterator = queue.iterator();
        while(iterator.hasNext()){
            Element tmp = iterator.next();
            context.write(new Text(tmp.geohash), new DoubleWritable(tmp.snow));
        }
    }

}