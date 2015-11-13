/*********************************************************************************************
* Author: Claudiu Barzu                        
* Date: 13/11/2015                             
* Description: This file implements the reducer.
**********************************************************************************************/

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class UpcReducer extends Reducer<Text,Text,Text,IntWritable> {
	/**
	* Given the key, which is the page (string), and values (string), return for each key how unque values
	* there are for each one.
	*/
	public void reduce(Text key, Iterable<IntWritable> values, Context contexto) throws IOException, InterruptedException {

	}
}
