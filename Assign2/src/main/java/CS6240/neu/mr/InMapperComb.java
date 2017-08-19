package CS6240.neu.mr;

/**
 * 
 *
 */
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class InMapperComb extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
	  BasicConfigurator.configure(); 
    int res = ToolRunner.run(new InMapperComb(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "InMapperComb");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(MyMap.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class MyMap extends Mapper<LongWritable, Text, Text, Text> {
	  
	  //protected void setup(Context context) throws IOException, InterruptedException 
	  
		  Map<String,Double> max_sum = new HashMap<String, Double>();   //global hashmap to store max temp with station Id as key
		  Map<String,Double> min_sum = new HashMap<String,Double>(); //global hashmap to store min temp with station Id as key
		  Map<String,Integer> max_count =  new HashMap<String , Integer>();// global hashmap for max count
		  Map<String,Integer> min_count = new HashMap<String,Integer>();  //global hashmap for min count

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
    	
       String line = lineText.toString();
       String tokens[] = line.split("[,]");  
              
      if (!StringUtils.isEmpty(tokens[3])){

	       if(tokens[2].equalsIgnoreCase("TMAX")){   	   // if it is max temp store the values(temp,count) in respective hashmap
	    	    double d = (Double.parseDouble(tokens[3]));
		    	   
			    	    if(max_sum.containsKey(tokens[0])){
			    	    	max_sum.put(tokens[0], (Double)max_sum.get(tokens[0])+d);
			    	    } 
			    	    else{
			    	    	max_sum.put(tokens[0],d);
			    	    }
			    	    
			    	    if(max_count.containsKey(tokens[0])){
			    	    	max_count.put(tokens[0], (Integer)max_count.get(tokens[0])+1);
			    	    }else{
			    	    	max_count.put(tokens[0], 1);
			    	    }
	    	       
	       }
	       
	       if(tokens[2].equalsIgnoreCase("TMIN")){   //// if it is  min temp store the values(temp,count) in respective hashmap
	    	    double d  = (Double.parseDouble(tokens[3]));
	    	   
		    	    if(min_sum.containsKey(tokens[0])){
		    	    	min_sum.put(tokens[0], (Double)min_sum.get(tokens[0])+d);
		    	    } 
		    	    else{
		    	    	min_sum.put(tokens[0],d);
		    	    }
		    	    if(min_count.containsKey(tokens[0])){
		    	    	min_count.put(tokens[0], (Integer)min_count.get(tokens[0])+1);
		    	    }else{
		    	    	min_count.put(tokens[0], 1);
		    	    } 
	    	    
	       }	       
      }
         
   }
    
    protected void cleanup(Context context) throws IOException, InterruptedException {  //clean up method to aggregate and emit final data
		Iterator<Map.Entry<String, Double>> itr1 = max_sum.entrySet().iterator();
		while (itr1.hasNext()) {   //aggregating the max temp from respective hashmap
			Entry<String, Double> entry1 = itr1.next();
			String station_id = entry1.getKey();
			Double station_max = entry1.getValue();
			Integer station_count = (Integer) max_count.get(station_id);
			String str = "MAX"+","+station_max+","+station_count;
			context.write(new Text(station_id), new Text(str));
		}
    
		Iterator<Map.Entry<String, Double>> itr2 = min_sum.entrySet().iterator();
		 while (itr2.hasNext()) {   //aggregating the min temp from respective hasmap
			Entry<String, Double> entry1 = itr2.next();
			String station_id = entry1.getKey();
			Double station_min = entry1.getValue();
			Integer station_count = (Integer)min_count.get(station_id);
			String str = "MIN"+","+station_min+","+station_count;  
			context.write(new Text(station_id), new Text(str));
		}
    }
 }
  
  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
 
        double max_sum = 0;
        double min_sum = 0;
        int min_count=0;
        int max_count=0;

        for (Text count : counts) {  //iterate through values
      	   String[] str = count.toString().split("[,]");
             
      	   if(str[0].equalsIgnoreCase("MAX")){  //parse max temp and calulate total sum and count
      		   double mx = Double.parseDouble(str[1]);
      		   max_sum += mx;
      		   int mx_count = Integer.parseInt(str[2]);
      		   max_count += mx_count;
      	   }
      	   
      	   if(str[0].equalsIgnoreCase("MIN")){  //parse min temp and calculate total sum and count
      		   double mn = Double.parseDouble(str[1]);
      		   min_sum += mn;
      		   int mn_count = Integer.parseInt(str[2]);
      		   min_count += mn_count;
      	   }
      	   
        } 
      
        //calculate average; -9999 is invalid temp according to documentation
		double min_avg = (min_count== 0)? -9999: min_sum/min_count;
		double max_avg = (max_count== 0)? -9999: max_sum/max_count;
        String s = " MIN:  "+min_avg+"  MAX:  "+max_avg;
        context.write(word, new Text(s));
    }
  }
}
