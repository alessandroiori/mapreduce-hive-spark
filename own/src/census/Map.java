package census;

import java.io.IOException;

/**
 * 
 * @author	Alessandro Iori
 * @date	31-03-2017
 *
 */

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Mapper<LongWritable, Text, Text, DoubleWritable>
 *  Input in the map <key,value>:
 *  - LongWritable : line number of the file
 *  - Text : the content of the line
 *  Output of the map <key,value>:
 *  - Text : marital status of individual
 *  - DoubleWritable : number of hours that individual work for week
 */
public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	
	@Override
	public void map(LongWritable key, Text value, Context context) 
			throws IOException, InterruptedException {
		// one line of the input file
		String line = value.toString();
		// all data of the single line
		String[] data = line.split(",");
		
		try {
			String maritalStatus = data[5];
			Double hrs = Double.parseDouble(data[12]);
			
			// write the <key, value> output
			context.write(new Text(maritalStatus), new DoubleWritable(hrs));
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		
	}
	
}