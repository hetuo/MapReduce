package edu.usfca.cs.mr.driest;

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
public class DriestReducer1
        extends Reducer<Text, Text, Text, Text> {

    protected String month = null;
    protected double minHumidity = Integer.MAX_VALUE;
    protected double maxHumidity = Integer.MIN_VALUE;
    protected double avgHumidity = 0.0;
    protected double totalPrecip = 0.0;


    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        month = key.toString();
        double sumPrecip = 0.0;
        double sumHumidity = 0.0;
        int num = 0;
        for (Text value : values){
            num++;
            String[] tokens = value.toString().split("\\t");
            double humidity = Double.parseDouble(tokens[0]);
            double precipitation = Double.parseDouble(tokens[1]);
            sumPrecip += precipitation;
            sumHumidity += humidity;

            if (humidity > maxHumidity)
                maxHumidity = humidity;
            if (humidity < minHumidity)
                minHumidity = humidity;
        }
        totalPrecip = sumPrecip;
        if (num != 0)
            avgHumidity = sumHumidity / num;

        context.write(new Text(month), new Text(Double.toString(maxHumidity)
            + "\t" + Double.toString(minHumidity)
            + "\t" + Double.toString(totalPrecip)
            + "\t" + Double.toString(avgHumidity)));
    }

}