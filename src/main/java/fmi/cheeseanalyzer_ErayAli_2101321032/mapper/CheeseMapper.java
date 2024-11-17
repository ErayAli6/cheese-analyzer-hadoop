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
		if (isHeader(key)) {
			return;
		}

		String[] columns = parseColumns(value.toString());
		replaceEmptyFields(columns);

		if (matchesFilters(columns)) {
			String outputKey = generateOutputKey(columns);
			processRecord(columns, outputKey, output);
		}
	}

	protected abstract void processRecord(String[] columns, String outputKey,
			OutputCollector<Text, DoubleWritable> output) throws IOException;

	private boolean isHeader(LongWritable key) {
		return key.get() == 0;
	}

	private String[] parseColumns(String value) {
		return value.split("\",\"");
	}

	private void replaceEmptyFields(String[] columns) {
		for (int i = 0; i < columns.length; i++) {
			if (columns[i].trim().isEmpty()) {
				columns[i] = "Unknown";
			}
		}
	}

	private boolean matchesFilters(String[] columns) {
		return (provCode.equals("All") || columns[1].equals(provCode))
				&& (category.equals("All") || columns[7].equals(category))
				&& (milkType.equals("All") || columns[8].equals(milkType));
	}

	private String generateOutputKey(String[] columns) {
		return String.format("%s-%s-%s", columns[1], columns[7], columns[8]);
	}
}