package cdr;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, LongWritable, Text, LongWritable> {
	
	@Override
	public void reduce(Text fromPhoneNumber, Iterable<LongWritable> durations,
			Reducer<Text, LongWritable, Text, LongWritable>.Context context) 
					throws IOException, InterruptedException{
		
		long sum = 0;
		for(LongWritable singleDuration : durations) {
			sum += singleDuration.get();
		}
		
		if(sum >= 60L){
			context.write(fromPhoneNumber, new LongWritable(sum));
		}
	}
}
