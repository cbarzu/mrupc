/*********************************************************************************************
* Author: Claudiu Barzu                        
* Date: 12/11/2015                             
* Description: This file implements the mapper. The input of the mapper is text,
* and the output is a dictorionary with the page as key, and the IPs as value.
**********************************************************************************************/


import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.conf.Configuration;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.JobContext;

public class UpcMapper extends Mapper<LongWritable, Text, Text, Text>{
	private static final Logger log = Logger.getLogger(UpcMapper.class);
	/**
	* Maps the input.
	* The output is a map with the page as key, and the IPs it is accessed from.
	*/
	public void map(LongWritable key, Text value, Context contexto) 
	throws IOException, InterruptedException {
		log.info("Entering mapper");
		String [] logs = value.toString().split(" ");
		log.info("Array length : " + logs.length);
		//onlye are computed get operatoins.
		if (logs[5].contains("GET")){
				String excluded = contexto.getConfiguration().get("excluido");
			if (excluded != null && logs[0].equals(excluded)){
				log.info("Writed <"  + excluded + "," + logs[0] + ">");
				contexto.write(new Text("EXCLUIDOS"), new Text(logs[0]));
			}else{
				log.info("Writed <"  + logs[6] + "," + logs[0] + ">");
				contexto.write(new Text(logs[6]), new Text(logs[0]));
			}
		}
		
		
	}
}
  
