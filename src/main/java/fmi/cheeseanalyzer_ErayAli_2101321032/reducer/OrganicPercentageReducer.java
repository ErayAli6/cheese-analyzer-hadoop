package fmi.cheeseanalyzer_ErayAli_2101321032.reducer;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class OrganicPercentageReducer extends CheeseReducer {

	@Override
	protected void processOutput(Text key, double sum, int count, OutputCollector<Text, DoubleWritable> output)
			throws IOException {
		double percentage = (sum / count) * 100;
		percentage = Math.round(percentage * 100.0) / 100.0;
		output.collect(new Text(key + " Organic Percentage:"), new DoubleWritable(percentage));
	}
}