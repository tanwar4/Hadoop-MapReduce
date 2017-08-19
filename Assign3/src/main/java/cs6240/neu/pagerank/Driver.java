package cs6240.neu.pagerank;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.BasicConfigurator;

public class Driver {

	/**
	 * Main Driver class
	 */
	static long total_nodes=0;

	public static void main(String[] args) throws ClassNotFoundException, InterruptedException, Exception {
		//Job1 : Parses the compressed data and created the outlink graph
		parsing(args[0],args[1]);    
		// Job2: Calculates the page rank  of entire graph. This task runs 10 times.
		calcPageRank(args[1],args[2]);
		// Job 3: This job finds the top 100 pages with highest pagerank   	
		findTopN(args[3],args[4]);
	}


	/*
	 * This method creates a Job which finds the Top 100 pages with highest pagerank
	 * Input: Output file of the 10th iteration of pagerank job
	 * Output: Top 100 page with highest page rank
	 * */
	private static void findTopN(String args, String args2) throws ClassNotFoundException, InterruptedException, Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "FindTop100Job");
		BasicConfigurator.configure(); 
		job.setJarByClass(Driver.class);

		Path inputpath = new Path (args);
		Path outputpath = new Path(args2);	
		outputpath.getFileSystem(conf).delete(outputpath,true);

		job.setMapperClass(TopNMapper.class);

		job.setMapOutputKeyClass(NullWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setReducerClass(TopNReducer.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);

		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);

		if(!job.waitForCompletion(true)){
			throw new Exception("Job Failed");
		}

	}

	/*
	 * This method creates a Job which calculates the page rank for the entire graph.
	 * Input : preprocessed file from Job1
	 * Output : Pagerank of each node in the graph alongwith
	 * */
	private static void calcPageRank(String args, String args2) throws ClassNotFoundException, InterruptedException, Exception {

		Configuration conf = new Configuration();

		Path inputpath = new Path (args);
		Path outputpath = new Path(args2);
		outputpath.getFileSystem(conf).delete(outputpath,true);
		outputpath.getFileSystem(conf).mkdirs(outputpath);
		double sinkMass=0.0;

		int iteration =1;
		while(iteration<=10){ // Pagerank job which runs for 10 times

			Path joboutputpath = new Path(outputpath,String.valueOf(iteration));

			//After each iteration sinkMass value is calculated which is passed to next iteration
			sinkMass =  pagerankiteration(inputpath,joboutputpath,sinkMass); 

			inputpath = joboutputpath;   //The output becomes the input for next iteration
			iteration++;       	
		}
	}

	/*
	 * This method creates the parsing job
	 * The compressed file is read and an adjacency List representation of graph is created.
	 * Preprocessing is done in this job with all duplicates nodes removed
	 * This job also creates the total no of nodes in the graph
	 * */
	private static void parsing(String input,String output){

		try {
			Configuration conf = new Configuration();
			Job job = Job.getInstance(conf, "PreProcessingJob");
			BasicConfigurator.configure(); 
			job.setJarByClass(Driver.class);

			job.setMapperClass(PreProcessingMapper.class);
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);

			job.setReducerClass(PreProcessingReducer.class);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);

			Path inputpath = new Path (input);
			Path outputpath = new Path(output);
			outputpath.getFileSystem(conf).delete(outputpath,true);

			FileInputFormat.addInputPath(job, inputpath);
			FileOutputFormat.setOutputPath(job, outputpath);

			if(!job.waitForCompletion(true)){
				throw new Exception("Job Failed");
			}

			total_nodes = job.getCounters().findCounter(PreProcessingReducer.Counter.NODE_COUNT).getValue();          
		} catch (Exception ex) {
			System.out.println("Erorr Message"+ ex.getMessage());
		}
	}
	
	/*
	 * Helper function for Page Rank Job
	 * THis method actually creates the PageRankJob
	 * This is called 10 times which refines the pagerank of each nodes
	 * */
	private static double pagerankiteration(Path inputpath2, Path joboutputpath, double sinkMass) throws ClassNotFoundException, InterruptedException, Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf, "PageRankJob");
		BasicConfigurator.configure(); 
		job.setJarByClass(Driver.class);

		job.getConfiguration().setInt("TotalNodes", (int)total_nodes);
		job.getConfiguration().setDouble("TotalSinkMass", (double)sinkMass);

		job.setMapperClass(PageRankMapper.class);
		job.setInputFormatClass(org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);


		job.setReducerClass(PageRankReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		Path inputpath =inputpath2;
		Path outputpath = joboutputpath;

		FileInputFormat.addInputPath(job, inputpath);
		FileOutputFormat.setOutputPath(job, outputpath);

		if(!job.waitForCompletion(true)){
			throw new Exception("Job Failed");
		}

		//Once the job finish we calculate the total sink mass which is calculated in the PageRankJob mapper
		// we retrieve the value and pass it in the next iteration
		long total_sink_sum = job.getCounters().findCounter(PageRankMapper.Counter.SINK_NODE_MASS_COUNT).getValue();

		double sink_mass =  (double) total_sink_sum/100000;
		return sink_mass;

	}

}
