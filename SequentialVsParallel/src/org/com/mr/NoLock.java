package org.com.mr;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ExecutorService;

import org.com.mr.LoaderRoutine;

public class NoLock {
	//data structure which will contain individual station name as key and list of temperatures as values
	static HashMap<String,ArrayList<Double>> accumulation_data = new HashMap<>();
	
	//structure to hold final output ; station id as key and avg temp as value
	static HashMap<String,Double> output = new HashMap<>();
	
	//no of processors availaible on a machine
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
			
			//no of partition needed to equally distribute data
			int partition_size = (int)Math.ceil((double)outputfile.size()/MAX_THREADS);       
			
			//creating a list of equal no of paritition of original items
			List<List<String>> partition = new ArrayList<>();
			for(int i=0; i<outputfile.size(); i+=partition_size){
				partition.add(outputfile.subList(i, Math.min(i+partition_size, outputfile.size())));
			}

			//Creating threads which can run concurrently ; no locking is performed on shared data
			ExecutorService executor = Executors.newFixedThreadPool(MAX_THREADS);
			for(int i=0; i<MAX_THREADS ; i++){
				final int index = i;   //anonymous class workaround
				executor.submit(new Runnable(){   		
					public void run(){	
						for(String str:partition.get(index)){
							String[] tokens = str.split("[,]");  //split by ,
							if(tokens[2].equalsIgnoreCase("TMAX") && !(tokens[3].isEmpty())){  //extract only records which has T-MAX
								int temp = Integer.parseInt(tokens[3]);
								//fill the data structure with station Id as key and values as an arraylist
								//which contains the count and total temperature
								if(version == 1){
									fibonacci(17);
								}
								accumulation_data.putIfAbsent(tokens[0], new ArrayList<Double>(Collections.nCopies(2, 0.0)));
								accumulation_data.get(tokens[0]).set(0,accumulation_data.get(tokens[0]).get(0)+1);
								accumulation_data.get(tokens[0]).set(1,accumulation_data.get(tokens[0]).get(1)+ temp);       		
							}
						}  
					}
				});
			}

			try{
				executor.shutdown();
				// wait for the threads to finish if necessary
				executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
			}catch(InterruptedException e){
				e.printStackTrace();
			}
			
			//print/store  the average temperature
			for(Map.Entry<String, ArrayList<Double>> entry :accumulation_data.entrySet()){
				String Key = entry.getKey();
				double count = entry.getValue().get(0);
				double temp_sum = entry.getValue().get(1);
				output.put(Key, ((double)temp_sum)/count); 
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
		
		System.out.println("No Lock Min:"+min +" Max: "+max +" Avg"+sum/10);
	}

	private static int fibonacci(int n) {
		if (n <= 1) return n;
		else return fibonacci(n-1) + fibonacci(n-2);

	}
}

