import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;


public class AvgTemperatureMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable>{

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, IntWritable> output, Reporter reporter)
			throws IOException {
		
		String[] line = value.toString().split(","); // splitting by ","
		String dataPart = line[1]; // Fetching Date
		String avgtemp = line[18]; // Fetching Temperature
		
		// checking that is temperature numberic-- if it is then convert to IntWritable..else ignore
		if(StringUtils.isNumeric(avgtemp)){
			output.collect(new Text(dataPart),new IntWritable(Integer.parseInt(avgtemp)));
		}
	}

}
