package fmi.cheeseanalyzer_ErayAli_2101321032.mapper;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

public abstract class CheeseMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {
	protected String provCode;
	protected String category;
	protected String milkType;

	@Override
	public void configure(JobConf job) {
		provCode = job.get("provCode");
		category = job.get("category");
		milkType = job.get("milkType");
	}

	@Override
	public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
			throws IOException {
		if (key.get() == 0) {
			return;
		}

		String[] columns = value.toString().split("\",\"");
		boolean matchesFilters = (provCode.equals("All") || columns[1].equals(provCode))
				&& (category.equals("All") || columns[7].equals(category))
				&& (milkType.equals("All") || columns[8].equals(milkType));

		if (matchesFilters) {
			String outputKey = String.format("%s-%s-%s", columns[1], columns[7], columns[8]);
			processRecord(columns, outputKey, output);
		}
	}

	protected abstract void processRecord(String[] columns, String outputKey,
			OutputCollector<Text, DoubleWritable> output) throws IOException;
}