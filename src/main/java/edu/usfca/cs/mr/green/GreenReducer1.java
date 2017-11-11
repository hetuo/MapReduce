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

/*
* Calculation of the wind power:
http://www.raeng.org.uk/publications/other/23-wind-turbine
Blade length, l = 50 m
Wind speed, v = 10 m/s
Air density, ρ = 1.23 kg/m3
Power Coefficient, Cp = 0.4

A = pi * l^2 = 8459 m^2
P = 1/2ρAv^3Cp = 1/2 * 1.23 * 8459 * 12^3 * 0.4 = 3.6MW


Solar power calculation reference:
https://scool.larc.nasa.gov/lesson_plans/CloudCoverSolarRadiation.pdf
P = 990 * (1-0.75F^3) watts/m2 , where F is the fraction of sky cloud cover
on a scale from 0.0 (0% no clouds) to 1.0 (100% complete coverage).

* */
public class GreenReducer1
        extends Reducer<Text, Text, Text, Text> {

    protected double calculateWind(double speed){
        double l = 50;
        double p = 1.23;
        double cp = 0.4;
        double A = 3.14 * l * l;
        return 0.5 * p * A * (speed * speed * speed) * 0.4;
    }

    protected double calculateSolar(double cloud){

        if (cloud > 100)
            System.out.println("Cloud......... " + cloud);
        return 990 * (1 - 0.75 * ((cloud / 100.0) * (cloud / 100.0) * (cloud / 100.0)));
    }

    @Override
    protected void reduce(
            Text key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        double windPower = 0.0;
        double solarPower = 0.0;
        double sum = 0.0;
        long num = 0;
        for (Text value : values){
            String[] tokens = value.toString().split("\\t");
            solarPower += (calculateSolar(Double.parseDouble(tokens[0])) / 1000);
            windPower += calculateWind(Double.parseDouble(tokens[1]) / 1000);
            num++;
        }
        if (num != 0){
            sum = windPower + solarPower;
            double d1 = windPower * 1000 / num;
            double d2 = solarPower * 1000 / num;
            double d3 = sum * 1000 / num;
            context.write(key, new Text(Double.toString(d1) +
                "\t" + Double.toString(d2) +
                "\t" + Double.toString(d3)));
        }
    }

}