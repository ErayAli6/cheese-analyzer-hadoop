package fmi.cheeseanalyzer_ErayAli_2101321032.reducer;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;

public abstract class CheeseReducer extends MapReduceBase
        implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    protected String calcType;

    @Override
    public void configure(JobConf job) {
        calcType = job.get("calcType");
    }

    @Override
    public void reduce(Text key, Iterator<DoubleWritable> values,
                       OutputCollector<Text, DoubleWritable> output,
                       Reporter reporter) throws IOException {
        double sum = 0.0;
        int count = 0;

        while (values.hasNext()) {
            sum += values.next().get();
            count++;
        }

        processOutput(key, sum, count, output);
    }

    protected abstract void processOutput(Text key, double sum, int count,
                                          OutputCollector<Text, DoubleWritable> output) throws IOException;
}