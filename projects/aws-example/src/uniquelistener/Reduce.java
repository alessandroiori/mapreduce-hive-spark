package uniquelistener;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 
 * @author	Alessandro Iori
 * @date	15-04-2017
 *
 */
public class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
	
	@Override
	public void reduce(IntWritable trackId, Iterable<IntWritable> users, 
			Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context context) 
			throws IOException, InterruptedException {
		
		Set<Integer> set =  new HashSet<Integer>();
		
		for(IntWritable userId : users) {
			set.add(userId.get());
		}
		
		IntWritable usersNumber = new IntWritable(set.size());	
		context.write(trackId, usersNumber);
	}
}
