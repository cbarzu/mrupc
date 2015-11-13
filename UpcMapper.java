/*********************************************************************************************
* Author: Claudiu Barzu                        
* Date: 12/11/2015                             
* Description: This file implements the mapper. The input of the mapper is text,
* and the output is a dictorionary with the page as key, and the IPs as value.
**********************************************************************************************/


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.util.StringTokenizer;
 

public class UpcMapper extends Mapper<LongWritable, Text, Text, Text>{

	/**
	* Maps the input.
	* The output is a map with the page as key, and the IPs it is accessed from.
	*/
	public void map(LongWritable key, Text value, Context contexto) 
	throws IOException, InterruptedException {

                
	}
}
  
