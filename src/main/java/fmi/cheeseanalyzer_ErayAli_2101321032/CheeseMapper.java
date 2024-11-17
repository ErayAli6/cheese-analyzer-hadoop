package fmi.cheeseanalyzer_ErayAli_2101321032;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class CheeseMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable>{

	 private String provCode;
	    private String category;
	    private String milkType;
	    private String calcType;
	    
	    @Override
	    public void configure(JobConf job) {
	        provCode = job.get("provCode");
	        category = job.get("category");
	        milkType = job.get("milkType");
	        calcType = job.get("calcType");
	    }
	    
	    @Override
	    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> output, Reporter reporter)
	            throws IOException {
	        if (key.get() == 0) return;
	        
	        String[] columns = value.toString().split("\",\"");
	        // Check if columns match our filters
	        boolean matchesFilters = 
	            (provCode.equals("All") || columns[1].equals(provCode)) &&
	            (category.equals("All") || columns[7].equals(category)) &&
	            (milkType.equals("All") || columns[8].equals(milkType));
	            
	        if (matchesFilters) {
	            String outputKey = String.format("%s-%s-%s", 
	                columns[1], // ManufacturerProvCode
	                columns[7], // CategoryTypeEn
	                columns[8]  // MilkTypeEn
	            );
	            
	            if (calcType.equals("Average Moisture")) {
	                // Output moisture percentage
	                try {
	                    double moisture = Double.parseDouble(columns[3]);
	                    output.collect(new Text(outputKey), new DoubleWritable(moisture));
	                } catch (NumberFormatException e) {
	                    // Skip invalid numbers
	                }
	            } else {
	                // Output organic status (1.0 for organic, 0.0 for non-organic)
	                double isOrganic = columns[6].equals("1") ? 1.0 : 0.0;
	                output.collect(new Text(outputKey), new DoubleWritable(isOrganic));
	            }
	        }
	    }
}
