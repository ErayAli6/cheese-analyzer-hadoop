package fmi.cheeseanalyzer_ErayAli_2101321032.mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class FilterMapper extends CheeseMapper {

    @Override
    protected void processRecord(String[] columns, String outputKey, OutputCollector<Text, DoubleWritable> output)
            throws IOException {
        // Exclude CheeseId (assumed to be in columns[0])
        String filteredOutput = String.join("\t", columns).replaceFirst("^[^\t]+\t", ""); // Remove the first column
        output.collect(new Text(filteredOutput), new DoubleWritable(0.0));
    }
}