package uniquelistener;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * 
 * @author	Alessandro Iori
 * @date	15-04-2017
 *
 */
public class Map extends Mapper<IntWritable, Text, IntWritable, IntWritable> {

	IntWritable userId = new IntWritable();
	IntWritable trackId = new IntWritable();

	public static enum COUNTERS {
		INVALID_RECORD_COUNT,
		VALID_RECORD_COUNT
	};

	@Override
	public void map(IntWritable key, Text value, 
			Mapper<IntWritable, Text, IntWritable, IntWritable>.Context context) 
					throws IOException, InterruptedException {

		String[] parts=value.toString().split("|");

		try {
			userId.set(Integer.parseInt(parts[0]));
			trackId.set(Integer.parseInt(parts[1]));
			
			if(parts.length == 5){
				//add counters for valid record
				context.getCounter(COUNTERS.VALID_RECORD_COUNT).increment(1L);
		
				context.write(trackId, userId);
			} else {
				//add counters for invalid record
				context.getCounter(COUNTERS.INVALID_RECORD_COUNT).increment(1L);
			}

		} catch (Exception e){
			System.out.println(e.getMessage());
		}		
	}
}
