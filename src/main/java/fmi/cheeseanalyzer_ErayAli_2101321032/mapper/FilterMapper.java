package fmi.cheeseanalyzer_ErayAli_2101321032.mapper;

import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import java.util.Arrays;

public class FilterMapper extends CheeseMapper {

    @Override
    protected void processRecord(String[] columns, String outputKey, OutputCollector<Text, DoubleWritable> output)
            throws IOException {
        // Exclude CheeseId (assumed to be in columns[0])
        String[] dataColumns = Arrays.copyOfRange(columns, 1, columns.length); // Remove the first column
        // Join the remaining columns with '-' as the delimiter
        String filteredOutput = String.join("-", dataColumns);
        // Emit the filtered output as key, with a placeholder value
        output.collect(new Text(filteredOutput), new DoubleWritable(0.0));
    }
}