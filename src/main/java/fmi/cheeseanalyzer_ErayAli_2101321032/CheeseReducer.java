package fmi.cheeseanalyzer_ErayAli_2101321032;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class CheeseReducer extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable>{

	 private String calcType;
	    
	    @Override
	    public void configure(JobConf job) {
	        calcType = job.get("calcType");
	    }
	    
	    @Override
	    public void reduce(Text key, Iterator<DoubleWritable> values, OutputCollector<Text, DoubleWritable> output,
	            Reporter reporter) throws IOException {
	        double sum = 0.0;
	        int count = 0;
	        
	        while (values.hasNext()) {
	            sum += values.next().get();
	            count++;
	        }
	        
	        if (calcType.equals("Average Moisture")) {
	            double average = sum / count;
	            output.collect(new Text(key + " Average Moisture: "), new DoubleWritable(Math.round(average * 100.0) / 100.0));
	        } else {
	            double percentage = (sum / count) * 100;
	            output.collect(new Text(key + " Organic Percentage: "), new DoubleWritable(Math.round(percentage * 100.0) / 100.0));
	        }
	    }
}
