package fmi.cheeseanalyzer_ErayAli_2101321032.mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;

public class OrganicPercentageMapper extends CheeseMapper {

	@Override
	protected void processRecord(String[] columns, String outputKey, OutputCollector<Text, DoubleWritable> output)
			throws IOException {
		double isOrganic = columns[6].equals("1") ? 1.0 : 0.0;

		if (milkType.equals("All")) {
			output.collect(new Text(provCode + "-" + category + "-AllMilkTypes"), new DoubleWritable(isOrganic));
		} else {
			output.collect(new Text(outputKey), new DoubleWritable(isOrganic));
		}
	}
}
