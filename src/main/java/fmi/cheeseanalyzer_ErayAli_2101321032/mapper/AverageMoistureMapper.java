package fmi.cheeseanalyzer_ErayAli_2101321032.mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class AverageMoistureMapper extends CheeseMapper {

	@Override
	protected void processRecord(String[] columns, String outputKey, OutputCollector<Text, DoubleWritable> output)
			throws IOException {
		try {
			double moisture = Double.parseDouble(columns[3]);
			if (provCode.equals("All")) {
				String aggregatedKey = "AllProvinces-" + category + "-" + milkType;
				output.collect(new Text(aggregatedKey), new DoubleWritable(moisture));
			} else {
				output.collect(new Text(outputKey), new DoubleWritable(moisture));
			}
		} catch (NumberFormatException e) {
			// Skip invalid numbers
		}
	}
}
