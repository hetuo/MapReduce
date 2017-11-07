package edu.usfca.cs.mr.driest;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by tuo on 05/11/17.
 */
public class DriestMapper1
        extends Mapper<LongWritable, Text, Text, Text> {

    protected  List<String> geohashs = new ArrayList<>();

    protected void initialGeohashs(){
        geohashs.add("9q8z");
        geohashs.add("9q8y");
        geohashs.add("9q8v");
        geohashs.add("9q8u");
        geohashs.add("9q9p");
        geohashs.add("9q9n");
        geohashs.add("9q9j");
        geohashs.add("9q9h");
        geohashs.add("9q9r");
        geohashs.add("9q9q");
        geohashs.add("9q9m");
        geohashs.add("9q9k");
    }

    protected String getMonth(String timestamp){
        SimpleDateFormat t = new SimpleDateFormat("yyyy-MM");
        long s = Long.parseLong(timestamp);
        return t.format(s);
    }

    protected void map(LongWritable key, Text value, Mapper.Context context)
            throws IOException, InterruptedException {
        // tokenize into words.
        if (geohashs.size() == 0)
            initialGeohashs();
        String[] tokens = value.toString().split("\\t");
        if (geohashs.contains(tokens[1].substring(0, 4)))
            context.write(new Text(getMonth(tokens[0])),  new Text(tokens[12] + "\t"+tokens[55]));
    }
}
