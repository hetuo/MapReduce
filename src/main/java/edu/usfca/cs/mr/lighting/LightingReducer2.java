package edu.usfca.cs.mr.lighting;

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
public class LightingReducer2
        extends Reducer<Text, Text, Text, Text> {

    PriorityQueue<Element> queue = new PriorityQueue<Element>(10, new Comparator<Element>() {
        @Override
        public int compare(Element e1, Element e2) {
            return e1.lighting.compareTo(e2.lighting);
        }
    });

    private class Element{
        public String geohash;
        public String lighting;

        public Element(String geohash, String lighting){
            this.geohash = geohash;
            this.lighting = lighting;
        }
    }

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        for (Text value : values){
            String[] tokens = value.toString().split("\\t");
            if (queue.size() < 3)
                queue.add(new Element(tokens[0], tokens[1]));
            else{
                Element tmp = queue.peek();
                if (tokens[1].compareTo(tmp.lighting) > 0){
                    queue.poll();
                    queue.add(new Element(tokens[0], tokens[1]));
                }
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Iterator<Element> iterator = queue.iterator();
        while (iterator.hasNext()){
            Element tmp = iterator.next();
            context.write(new Text("lighting"), new Text(tmp.geohash + "    " + tmp.lighting));
        }
    }

}