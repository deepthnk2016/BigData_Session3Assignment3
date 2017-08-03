package com.acadgild;

/**
 * Mapper class to filter out records having NA in company name or product name
 * and list down company name and their count.
 */
import java.io.IOException;

//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;

public class CompanyWiseSaleMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

		// Split the input strings from the file on ","
		String[] lineArray = value.toString().split("\\|");

		// Fetch the company name and product name
		String companyName = lineArray[0];
		String prodName = lineArray[1];

		String outputText = companyName;
		// Initialize a variable to hold the output.
		Text output = new Text();
		output.set(outputText);

		// If the company name or product name contains NA, then don't print that line.
		if (companyName.equals("NA") == false && prodName.equals("NA") == false) {
			// write the output
			context.write(output, new LongWritable(1));
		}
	}
}
