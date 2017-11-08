package edu.usfca.cs.mr.green;

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
public class GreenReducer2
        extends Reducer<Text, Text, Text, Text> {


    PriorityQueue<Element> queueWind = new PriorityQueue<Element>(10, new Comparator<Element>() {
        @Override
        public int compare(Element e1, Element e2) {
            return e1.wind.compareTo(e2.wind);
        }
    });

    PriorityQueue<Element> queueSolar = new PriorityQueue<Element>(10, new Comparator<Element>() {
        @Override
        public int compare(Element e1, Element e2) {
            return e1.solar.compareTo(e2.solar);
        }
    });

    PriorityQueue<Element> queueSum = new PriorityQueue<Element>(10, new Comparator<Element>() {
        @Override
        public int compare(Element e1, Element e2) {
            return e1.sum.compareTo(e2.sum);
        }
    });

    private class Element{
        public String geohash;
        public String wind;
        public String solar;
        public String sum;

        public Element(String geohash, String wind, String solar, String sum){
            this.geohash = geohash;
            this.wind = wind;
            this.solar = solar;
            this.sum = sum;
        }
    }

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        String lighting = null;
        for (Text value : values){
            String[] tokens = value.toString().split("\\t");
            if (queueWind.size() < 3)
                queueWind.add(new Element(tokens[0], tokens[1], tokens[2], tokens[3]));
            else{
                Element tmp = queueWind.peek();
                if (tokens[1].compareTo(tmp.wind) > 0){
                    queueWind.poll();
                    queueWind.add(new Element(tokens[0], tokens[1], tokens[2], tokens[3]));
                }
            }

            if (queueSolar.size() < 3)
                queueSolar.add(new Element(tokens[0], tokens[1], tokens[2], tokens[3]));
            else{
                Element tmp = queueSolar.peek();
                if (tokens[2].compareTo(tmp.solar) > 0){
                    queueSolar.poll();
                    queueSolar.add(new Element(tokens[0], tokens[1], tokens[2], tokens[3]));
                }
            }

            if (queueSum.size() < 3)
                queueSum.add(new Element(tokens[0], tokens[1], tokens[2], tokens[3]));
            else{
                Element tmp = queueSum.peek();
                if (tokens[3].compareTo(tmp.sum) > 0){
                    queueSum.poll();
                    queueSum.add(new Element(tokens[0], tokens[1], tokens[2], tokens[3]));
                }
            }
        }
    }

    @Override
    protected void cleanup(Reducer<Text, Text, Text, Text>.Context context)
            throws IOException, InterruptedException {
        Iterator<Element> iterator = queueWind.iterator();
        while (iterator.hasNext()){
            Element tmp = iterator.next();
            context.write(new Text("Best wind"), new Text(tmp.geohash + "\t" + tmp.wind));
        }

        Iterator<Element> iterator1 = queueSolar.iterator();
        while (iterator1.hasNext()){
            Element tmp = iterator1.next();
            context.write(new Text("Best solar"), new Text(tmp.geohash + "\t" + tmp.solar));
        }

        Iterator<Element> iterator2 = queueSum.iterator();
        while (iterator2.hasNext()){
            Element tmp = iterator2.next();
            context.write(new Text("Best sum"), new Text(tmp.geohash + "\t" + tmp.sum));
        }
    }

}