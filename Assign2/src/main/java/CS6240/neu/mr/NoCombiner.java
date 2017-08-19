package CS6240.neu.mr;

/**
 * Hello world!
 *
 */
import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;

public class NoCombiner extends Configured implements Tool {
  private static final Logger LOG = Logger.getLogger(NoCombiner.class);
  public static void main(String[] args) throws Exception {
	  BasicConfigurator.configure(); 
    int res = ToolRunner.run(new NoCombiner(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "NoCombiner");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    //job.setCombinerClass(Reduce.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
       String line = lineText.toString();
       String tokens[] = line.split("[,]");    //splitting lines into token to retrieve staion id and temp
              
      if (!StringUtils.isEmpty(tokens[3])){   //only proceed if temperature is  present in record

           String str=null;
	       if(tokens[2].equalsIgnoreCase("TMAX")){   	    
	    	    double d = (Double.parseDouble(tokens[3]));
	    	     str = "MAX"+","+d;						//append MAX with extracted temp
	       }
	       if(tokens[2].equalsIgnoreCase("TMIN")){              //checking type of temp
	    	    double d  = (Double.parseDouble(tokens[3]));
	    	     str = "MIN"+","+d;    	               //append MIN with extracted temp
	       }	       
	       
	       if(str!=null){
	    	   context.write(new Text(tokens[0]), new Text(str));  //emit the station Id and appended string
	       }

      }
       
     }
  }
  

  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
 
      double max_sum = 0;    //Initializing variables 
      double min_sum = 0;
      int min_count=0;
      int max_count=0;

    //iterate through the list and calculate  total max_temp and total min temp per station id
      for (Text count : counts) {  
    	   String[] str = count.toString().split("[,]");
           
    	   if(str[0].equalsIgnoreCase("MAX")){
    		   double mx = Double.parseDouble(str[1]);
    		   max_sum += mx;
    		   max_count++;
    	   }
    	   
    	   if(str[0].equalsIgnoreCase("MIN")){
    		   double mn = Double.parseDouble(str[1]);
    		   min_sum += mn;
    		   min_count++;
    	   }
    	   
      } 	  
      
      // -9999 is considered as invalid according to documentation
      //calculate min avg and max avg
	  double min_avg = (min_count== 0)? -9999: min_sum/min_count;
	  double max_avg = (max_count== 0)? -9999: max_sum/max_count;
      String s = " MIN:  "+min_avg+"  MAX:  "+max_avg;
      context.write(word, new Text(s));
    }
  }
}
