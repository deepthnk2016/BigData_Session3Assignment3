package com.acadgild;

/**
 * Reducer class to sum up the sales of television based on the key-pair value.
 * In one case key is company name
 * In the second case key is concatenated string of company name and state. 
 */
import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TelevisionSaleReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

	public void reduce(Text key, Iterable<LongWritable> itr, Context context) throws IOException, InterruptedException {
		int sum = 0;
		for (LongWritable value : itr) {
			sum += value.get();
		}
		context.write(key, new LongWritable(sum));
	}
}
