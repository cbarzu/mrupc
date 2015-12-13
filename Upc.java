
/*********************************************************************************************
* Author: Claudiu Barzu                        
* Date: 13/11/2015                             
* Description: This file is the main class, which we use to configure map reduce and run
* the program.
**********************************************************************************************/


import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.conf.Configured;
import org.apache.log4j.Logger;

public class Upc extends Configured implements Tool {
    private static final Logger log = Logger.getLogger(Upc.class);

    public int run(String[] args) throws Exception {
		if (args.length < 2) {
      			System.err.println("Usage:java Upc <in> [<in>...] <out>");
			System.exit(2);
		}
		Job job = Job.getInstance(getConf());

		job.setJarByClass(getClass());
		//Setting up the mapper and reducer class
		job.setMapperClass(UpcMapper.class);
		job.setReducerClass(UpcReducer.class);
		//Setting the mapper ouput tipe of key and value
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// setting the number of reducers.
		job.setNumReduceTasks(2);
		log.info("Length of args " + args.length);
		log.info("Args[0] " + args[0]);
		log.info("exclude=" + getConf().get("excluido"));

		for (int i = 0; i < args.length-1; ++i)
			FileInputFormat.addInputPath(job, new Path(args[i]));
    	FileOutputFormat.setOutputPath(job, new Path(args[args.length-1]));

		return job.waitForCompletion(true) ? 0 : 1;
	}




    public static void main(String[] args) throws Exception {
            int resultado = ToolRunner.run(new Upc(), args);
            System.exit(resultado);
    }
}
