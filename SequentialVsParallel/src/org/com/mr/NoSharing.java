package org.com.mr;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;

import org.com.mr.LoaderRoutine;
import org.omg.Messaging.SyncScopeHelper;

public class NoSharing{
	//data structure which will contain individual station name as key and list of temperatures as values
	static HashMap<String,ArrayList<Double>> accum_data = new HashMap<>();
	
	//structure to hold final output ; station id as key and avg temp as value
	static HashMap<String,Double> output = new HashMap<>();

	private static final int MAX_THREADS = Runtime.getRuntime().availableProcessors();
	public static void main(String[] args) {
	  	String inputfile = args[0];
	  	int version= Integer.parseInt(args[1]);
    	// Loading data from Loader Routine into output file data structure
		List<String> outputfile = LoaderRoutine.readfile(inputfile);
		
		int j=0;
		double[] arr = new double[10]; 
		while(j<10){    // run the calculation 10 times to analyze avg running time 
		long start= System.currentTimeMillis();
		//no of partition needed 
		int partition_size = (int)Math.ceil((double)outputfile.size()/MAX_THREADS);       

		//creating a list of equal no of paritition of original items
		List<List<String>> partition = new ArrayList<>();
		for(int i=0; i<outputfile.size(); i+=partition_size){
			partition.add(outputfile.subList(i, Math.min(i+partition_size, outputfile.size())));
		}

		//Creating threads which can run concurrently
		//Every individual thread will work on their own data structure and return the data to the main 
		//thread using Future pattern
		ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);
		Collection<Callable<HashMap<String,ArrayList<Double>>>> tasks = new ArrayList<>(); //list of task

		for(int i=0; i<MAX_THREADS ; i++){	
			final int index = i;
			tasks.add(new Callable<HashMap<String,ArrayList<Double>>>(){    //task created
				public HashMap<String,ArrayList<Double>> call() 
						throws Exception
				{
					HashMap<String,ArrayList<Double>> accumulation_data = new HashMap<>();	//separate data structure to work on by each thread
					for(String str:partition.get(index)){
						String[] tokens = str.split("[,]");
						if(tokens[2].equalsIgnoreCase("TMAX") && !(tokens[3].isEmpty())){  //extract only records which has T-MAX
							int temp = Integer.parseInt(tokens[3]);
							//fill the data structure with station Id as key and values as an arraylist
							//which contains the count and total temperature	
							if(version == 1){
								int x = fibonacci(17);
								}
							accumulation_data.putIfAbsent(tokens[0], new ArrayList<Double>(Collections.nCopies(2, 0.0)));
							accumulation_data.get(tokens[0])
											 .set(0,accumulation_data.get(tokens[0]).get(0)+1);
							accumulation_data.get(tokens[0])
											 .set(1,accumulation_data.get(tokens[0]).get(1)+ temp);       				

						}
					} 
					return accumulation_data;  // return the data to main thread
				}
			});
		}		
		//Invoke all will wait for all threads to finish execution 
		//grab all the results from future and store the accumulated data into single datastructure
		try {
			List<Future<HashMap<String,ArrayList<Double>>>> results = executor.invokeAll(tasks, Long.MAX_VALUE, TimeUnit.SECONDS);
			for(Future<HashMap<String,ArrayList<Double>>> f:results){
				HashMap<String,ArrayList<Double>> hm = f.get();
				//Accumulate the data returned by each thread in the main datastructure
				for(Map.Entry<String, ArrayList<Double>> entry :hm.entrySet()){
					String Key = entry.getKey();
					//storing the data from all threads into single global datastructure
					accum_data.putIfAbsent(Key, entry.getValue());
					accum_data.get(Key).set(0, entry.getValue().get(0)+ accum_data.get(Key).get(0));
					accum_data.get(Key).set(1, entry.getValue().get(1)+ accum_data.get(Key).get(1));
				}

			}
			executor.shutdown(); 			
			executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (ExecutionException e) {
			e.printStackTrace();
		}
		finally {    
			//print the average temperature
			for(Map.Entry<String, ArrayList<Double>> entry :accum_data.entrySet()){
				String Key = entry.getKey();
				double count = entry.getValue().get(0);
				double temp_sum = entry.getValue().get(1);
				output.put(Key,((double)temp_sum)/count);
			}			
		}
		long stop= System.currentTimeMillis();
		double time=(stop-start)*.001;
		arr[j]=time;
		j++;
	}
		//calculation of min max and average
		double min=arr[0],max=arr[0],sum=0;
		for(int i=0;i<9;i++){
			if(arr[i]<min) min=arr[i];
			if(arr[i]>max) max=arr[i];
			sum+=arr[i];
		}

		System.out.println(" No Sharing:  Min:"+min +" Max: "+max +" Avg"+sum/10);
	}
	private static int fibonacci(int n) {
		if (n <= 1) return n;
		else return fibonacci(n-1) + fibonacci(n-2);

	}
}


