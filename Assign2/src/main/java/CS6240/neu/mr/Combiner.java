package CS6240.neu.mr;

/**
 * 
 *
 */
import java.io.IOException;
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

public class Combiner extends Configured implements Tool {
  public static void main(String[] args) throws Exception {
	  BasicConfigurator.configure(); 
    int res = ToolRunner.run(new Combiner(), args);
    System.exit(res);
  }

  public int run(String[] args) throws Exception {
    Job job = Job.getInstance(getConf(), "Combiner");
    job.setJarByClass(this.getClass());
    // Use TextInputFormat, the default unless job.setInputFormatClass is used
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));
    job.setMapperClass(Map.class);
    job.setCombinerClass(MyCombiner.class);
    job.setReducerClass(Reduce.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }
  
  public static class Map extends Mapper<LongWritable, Text, Text, Text> {

    public void map(LongWritable offset, Text lineText, Context context)
        throws IOException, InterruptedException {
       String line = lineText.toString();
       String tokens[] = line.split("[,]");  
              
      if(tokens.length >0){     //checking for validity of retrieved tokens
	       if (!StringUtils.isEmpty(tokens[3])){
	
	    	   
	    	   //The output format to be emited is (Mintemp,Mintemp cout, Max temp, Maxtemp count)
	           String str=null;
		       if(tokens[2].equalsIgnoreCase("TMAX")){   	   
		    	    double d = (Double.parseDouble(tokens[3]));
		    	     str = "0" + "," + "0" + "," +d+ "," +"1";           //min temp and min temp count will be 0
		       }
		       if(tokens[2].equalsIgnoreCase("TMIN")){
		    	    double d  = (Double.parseDouble(tokens[3]));
		    	    str = d+ "," + "1" + "," + "0" + "," + "0";    	   //here maxtemp and maxtemp count will be 0   
		       }	       
		       
		       if(str!=null){
		    	   context.write(new Text(tokens[0]), new Text(str));   //emit
		       }
	
	      }
     }    
       
     }
  }
  
  //Combiner class to combine data
  //the input and output format should be the same such that in case if no combiner is involved program should work fine
  public static class MyCombiner extends Reducer<Text, Text,Text,Text>{
	    public void reduce(Text word, Iterable<Text> counts, Context context)
	            throws IOException, InterruptedException {
	    	
	        double max_sum = 0;
	        double min_sum = 0;
	        int min_count=0;
	        int max_count=0;
	       

	        for (Text count : counts) {  //iterate through values
	      	   String[] str = count.toString().split("[,]");
	       	   
	    	   
	      	   //extract and combine respective values in the same order as provided by mapper
	      	//The format received is (Mintemp,Mintemp cout, Max temp, Maxtemp count)
	      	   
    		   double mn = Double.parseDouble(str[0]);
    		   min_sum += mn;
    		   
    		   int mn_count = Integer.parseInt(str[1]);
    		   min_count += mn_count;
    	   
    		   double mx = Double.parseDouble(str[2]);
    		   max_sum += mx;
    	   
    		   int mx_count = Integer.parseInt(str[3]); 
    	       max_count += mx_count;
	      	  
	        } 
	        
	        String str = min_sum+","+min_count+","+max_sum+","+max_count; //create same order as emitted by Mapper
	        Text ctext = new Text(str);
	        context.write(word, new Text(ctext));
	    	
	    }
  }
  
  public static class Reduce extends Reducer<Text, Text, Text, Text> {
    @Override
    public void reduce(Text word, Iterable<Text> counts, Context context)
        throws IOException, InterruptedException {
 
      double max_sum = 0;
      double min_sum = 0;
      int max_count=0;
      int min_count=0;
      
      for (Text count : counts) {  //iterate throgh values
    	   String[] str = count.toString().split("[,]");
           
      	 //extract and reduce respective values 
      	//The format received is (Mintemp,Mintemp cout, Max temp, Maxtemp count ) 
    	//to ensure in case no combiner is involved ,the program work just fine   
    	   
    		   double mn = Double.parseDouble(str[0]);
    		   min_sum += mn;
    		   
    		   int mn_count = Integer.parseInt(str[1]);
    		   min_count += mn_count;
    	   
    		   double mx = Double.parseDouble(str[2]);
    		   max_sum += mx;
    	   
    		   int mx_count = Integer.parseInt(str[3]); 
    	       max_count += mx_count;
      } 	  
      
		double min_avg = (min_count== 0)? -9999: min_sum/min_count;
		double max_avg = (max_count== 0)? -9999: max_sum/max_count;
        String s = " MIN:  "+min_avg+"  MAX:  "+max_avg;
        context.write(word, new Text(s));  //emit ouput
    }
  }
}
