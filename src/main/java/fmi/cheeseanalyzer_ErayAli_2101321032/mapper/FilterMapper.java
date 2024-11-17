package fmi.cheeseanalyzer_ErayAli_2101321032.mapper;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.Arrays;

public class FilterMapper extends CheeseMapper {

    @Override
    protected void processRecord(String[] columns, String outputKey, OutputCollector<Text, DoubleWritable> output)
            throws IOException {
        String[] dataColumns = Arrays.copyOfRange(columns, 1, columns.length); // Remove the first column
        String filteredOutput = String.join("-", dataColumns);
        output.collect(new Text(filteredOutput), new DoubleWritable(0.0));
    }
}