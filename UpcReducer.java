/*********************************************************************************************
* Author: Claudiu Barzu                        
* Date: 13/11/2015                             
* Description: This file implements the reducer.
**********************************************************************************************/

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import java.util.Iterator;
import java.util.HashSet;
import java.util.Set;

public class UpcReducer extends Reducer<Text,Text,Text,IntWritable> {
	private static final Logger log = Logger.getLogger(UpcReducer.class);
	/**
	* Given the key, which is the page (string), and values (string), return for each key how unque values
	* there are for each one.
	*/
	public void reduce(Text key, Iterable<Text> values, Context contexto) throws IOException, InterruptedException {
		log.info("Entering reducer");
		log.info("The key is " + key.toString());
		Iterator<Text> iter = values.iterator();
		Set<String> copy = 	new HashSet<>();
		while (iter.hasNext()){
			String ip = iter.next().toString();
    		log.info("The ips in reducer is :" + ip);
    		copy.add(ip);
    	}
		
		contexto.write(key, new IntWritable(copy.size()));
	}
}
