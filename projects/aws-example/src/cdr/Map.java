package cdr;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import cdr.CdrRecord;

public class Map extends Mapper<Object, Text, Text, LongWritable> {

	Text fromPhoneNumber = new Text();
	LongWritable duration = new LongWritable();

	@Override
	public void map(Object key, Text value, 
			Mapper<Object, Text, Text, LongWritable>.Context context) 
					throws IOException, InterruptedException {

		String[] parts = value.toString().split("[|]");

		if(parts.length == CdrRecord.CDR_RECORD_LENGTH) {
			try {

				if(parts[CdrRecord.STDF_FLAG].equalsIgnoreCase("1")) {

					String startTime = parts[CdrRecord.CALL_START_TIME];
					String endTime = parts[CdrRecord.CALL_END_TIME];

					long diff = getDateDiff(startTime, endTime, TimeUnit.MINUTES);

					fromPhoneNumber.set(parts[CdrRecord.FROM_PHONE_NUMBER]);
					duration.set(diff);
					context.write(fromPhoneNumber, duration);
				}
			} catch (Exception e){
				System.out.println(e.getMessage());
			}
		}
	}

	public static long getDateDiff(String date1, String date2, TimeUnit timeUnit) {
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date d1 = formatter.parse(date1);
			Date d2 = formatter.parse(date2);
			long diffInMillies = d2.getTime() - d1.getTime();
			return timeUnit.convert(diffInMillies,TimeUnit.MILLISECONDS);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return 0L;
	}
}
