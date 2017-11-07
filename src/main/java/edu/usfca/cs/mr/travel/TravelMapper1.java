package edu.usfca.cs.mr.travel;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

public class TravelMapper1
        extends Mapper<LongWritable, Text, Text, Text> {

    protected List<String> geohashs = new ArrayList<>();

    protected void initialGeohashs(){
        geohashs.add("c6");
        geohashs.add("9q");
        geohashs.add("dr");
        geohashs.add("ws");
        geohashs.add("yb");
    }

    protected String getMonth(String timestamp){
        SimpleDateFormat t = new SimpleDateFormat("MM");
        long s = Long.parseLong(timestamp);
        return t.format(s);
    }

    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        if (geohashs.size() == 0)
            initialGeohashs();
        String[] tokens = value.toString().split("\\t");
        if (geohashs.contains(tokens[1].substring(0, 2)))
            context.write(new Text(tokens[1].substring(0,2)),  new Text(getMonth(tokens[0]) + "\t"+tokens[40]));
    }
}
