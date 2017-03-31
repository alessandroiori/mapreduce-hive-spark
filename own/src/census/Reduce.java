package census;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * Reducer<Text, DoubleWritable, Text, DoubleWritable>
 *  Input in the reduce <key,value>:
 *  - Text : key from map phase
 *  - DoubleWritable: value from map phase
 *  Output of the reduce <key,value>:
 *  - Text : key output of the reduce phase
 *  - DoubleWritable: output value of reduce phase
 */
public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	/**
	 * Iterable object becouse, befoure reduce phase, collects all value with the same key
	 * 
	 * @param key
	 * @param values
	 * @param contex
	 */
	@Override
	public void reduce(final Text key,
			final Iterable<DoubleWritable> values,
			final Context context) 
					throws IOException, InterruptedException {
		Double sum = 0.0;
		Integer count = 0;
		for (DoubleWritable value : values) {
			sum += value.get();
			count++;
		}
		Double ratio = sum / count;
		context.write(key, new DoubleWritable(ratio));
	}
}
