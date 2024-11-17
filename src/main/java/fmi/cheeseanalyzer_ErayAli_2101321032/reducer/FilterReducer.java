package fmi.cheeseanalyzer_ErayAli_2101321032.reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class FilterReducer extends CheeseReducer {

    @Override
    protected void processOutput(Text key, double sum, int count, OutputCollector<Text, DoubleWritable> output)
            throws IOException {
        // Output the filtered record without CheeseId
        output.collect(key, new DoubleWritable(0.0));
    }
}