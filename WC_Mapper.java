package org.mapReducer;
import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class WC_Mapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text> {
    private final Text outKey = new Text();
    private final Text outValue = new Text();

    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
                    Reporter reporter) throws IOException {
        // skip the first line if it starts with "header"
        if (key.get() == 0) {
            return;
        }

        String line = value.toString().replaceAll(String.valueOf('"'), "");
        String[] tokens = line.split("\t");

        outKey.set(tokens[0] + "\t" + tokens[1]);
        outValue.set(tokens[2].split("/")[0]);

        output.collect(outKey, outValue);
    }
}