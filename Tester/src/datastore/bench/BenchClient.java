package datastore.bench;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import smartkv.server.bench.TestInformation;
import bftsmart.tom.ServiceProxy;
import bftsmart.tom.TimeoutException;
import datastore.bench.flowsimulations.FlowSimulation;
import datastore.bench.flowsimulations.deviceManager.WorkloadPerFlow;


public abstract class BenchClient implements Runnable{
	
	static long test_start_time = System.currentTimeMillis();
	public static void main(String[] args) throws InterruptedException{
		if (args.length < 3) {
            System.out.println("Usage: ... ThroughputLatencyClient <num. threads> <number of operations per thread> <interval> <verbose?> <stored statistics?> <start_id> <CASE (see above)?>");
            System.out.println(MultiFlowTypes.getValues()); 
            System.exit(-1);
        }
		
	    int numThreads = Integer.parseInt(args[0]);
        int numberOfFlows = Integer.parseInt(args[1]); 
        boolean verbose =Boolean.parseBoolean(args[3]);
        long interval = Integer.parseInt(args[2]); 
        boolean storedStatistics = Boolean.parseBoolean(args[4]);
        int start_id = Integer.parseInt(args[5]);
        
        String type = args[6];
        
        if (!MultiFlowTypes.simulations.containsKey(type) && !type.equals("all")){
        	System.out.println("invalid test case."); 
        	System.out.println(MultiFlowTypes.getValues()); 
        	System.exit(0); 
        }
        
        
        if (!type.equals("all")){
        	performTest(numThreads, numberOfFlows, verbose, interval,
    				storedStatistics, start_id, type);
            }
        else{
        	for (String t : MultiFlowTypes.simulations.keySet()){
        		performTest(numThreads, numberOfFlows, verbose, interval,
        				storedStatistics, start_id, t);
        	}
        }
        System.exit(0);
        
	}

	/**
	 * @param numThreads
	 * @param numberOfFlows
	 * @param verbose
	 * @param interval
	 * @param storedStatistics
	 * @param start_id
	 * @param type
	 */
	private static void performTest(int numThreads, int numberOfFlows,
			boolean verbose, long interval, boolean storedStatistics,
			int start_id, String type) {
		DescriptiveStatistics[] stats = new DescriptiveStatistics[numThreads];
        
        for(int i=0; i<numThreads; i++) {
            stats[i] = new DescriptiveStatistics(); 
        }
        
        boolean end = createAndWaitForThreads(numThreads, numberOfFlows, verbose, interval,
				storedStatistics, start_id, type, stats);
        if (!end) System.exit(-1); 
        
        Long random = sendFinalSignToServer(type,numThreads, start_id);
        printResultsAndFinish(numThreads, stats,type, random, numberOfFlows);
        System.exit(0); 
	}



	private static Long sendFinalSignToServer(String name, int threads, int start_id){
		ServiceProxy proxy = new ServiceProxy(start_id);
		TestInformation t = new TestInformation(name,threads);
		
		ByteArrayOutputStream out = new ByteArrayOutputStream(); 
		DataOutputStream dos = new DataOutputStream(out);
		try {		
		dos.writeInt(-3);
		byte[] s =serialize(t); 
		dos.writeInt(s.length); 
		dos.write(s); 
		dos.flush(); dos.close();
		proxy.invokeOrdered(out.toByteArray());
		proxy.close(); 
		} catch (IOException e) {
			return null;
		} 
		return t.random;
	}

	/**
	 * @param numThreads
	 * @param stats
	 * @param signaled 
	 */
	private static void printResultsAndFinish(int numThreads,
			DescriptiveStatistics[] stats,String type, Long random, int ops) {
		SummaryStatistics statsEnd = new SummaryStatistics(); 
        for (int i = 0 ; i < numThreads ; i++){
        	  double[] vals = stats[i].getSortedValues();
        	  int len = (int) (vals.length *( 0.95));
        	  for (int j = 0 ; j < len ; j++ ){
        		  statsEnd.addValue(vals[j]);
        	  }
        }
        try{
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("./latency", true)));
        out.print( type +":" + numThreads + ":"+ statsEnd.getN() + ":" + statsEnd.getMean() + ":" +statsEnd.getStandardDeviation() + ":" + statsEnd.getMin() + ":" + statsEnd.getMax() + ":" + random 
        		 + ":" +  ops);
        //Print Test information. 
        out.print(":("); 
        WorkloadPerFlow f = MultiFlowTypes.simulations.get(type);
        out.print(f.workloadDescription +";"); 
        
        int[][] requests = f.requests;  
        for (int i = 0 ; i < requests.length ; i++){
        	out.print(requests[i][0] == FlowSimulation.WRITE_OP ? "W" : "R"); 
        	out.print(","); 
        	out.print(requests[i][1] + "," +requests[i][2]);
        		if ((i + 1) < requests.length){
        			out.print(";");
        		}	
        }
        out.print(")");
        
        //Test duration
        out.print(":" + ((System.currentTimeMillis() - test_start_time) / (1000 *60) ));
        out.println(""); 
        
        out.close(); 
        System.out.println("Printed results"); 
        }catch(Exception e ){
        	System.out.println("failed to write"); 
        }
	}
		
	/**
	 * @param numThreads
	 * @param numberOfFlows
	 * @param verbose
	 * @param interval
	 * @param storedStatistics
	 * @param start_id
	 * @param type
	 * @param stats
	 */
	private static boolean createAndWaitForThreads(int numThreads,
			int numberOfFlows, boolean verbose, long interval,
			boolean storedStatistics, int start_id, String type,
			DescriptiveStatistics[] stats) {
		
		Thread[] c = new Thread[numThreads];
        Boolean[] end_condition = new Boolean[numThreads];
        if (verbose){
        	System.out.println("Creating threads"); 
        }
        
        for(int i=0; i<numThreads; i++) {
            c[i] = new Thread(new MultiFlowTypes(i, numberOfFlows, verbose, interval, storedStatistics, start_id,type, stats[i], end_condition)); 
        }
        
        for(int i=0; i<numThreads; i++) {
            c[i].start();
        }
        
        if (verbose){
        	System.out.println("Threads created... "); 
        }
        
        for(int i=0; i<numThreads; i++) {
            try {
                c[i].join();
            } catch (InterruptedException ex) {
                ex.printStackTrace(System.err);
            }
        }
        
        int sum=0; 
        try{
        for (Boolean o : end_condition){
        	if (o) sum++; 
        }
        }catch(NullPointerException e){
        	; 
        }
        
        if (sum > ((2.0/3.0 ) * numThreads)){
        	return true;
        }
        else return false; 
	}
	
	protected final ServiceProxy proxy ; 
	protected  final int id;
	protected final DescriptiveStatistics latency;
	protected final int numFlows;
	protected final boolean verbose;  
	protected final long interval;
	
	protected final  int start_id;
	private Boolean[] end_condition;
	//protected SummaryStatistics latency2 = new SummaryStatistics();
	public BenchClient(int id, int numFlows, boolean verbose, long interval, boolean storedStatistics, int start_id, String type, DescriptiveStatistics stats, Boolean[] end_condition){
		this.id = id; 
		proxy = new ServiceProxy(start_id + id); 
		this.numFlows = numFlows; 
		this.verbose = verbose; 
		this.interval = interval;
		this.end_condition = end_condition; 
		this.start_id = start_id;
		this.latency = stats; 
		if (verbose)
			System.out.println("Starting thread :" + id);
		
	}
	
	@Override
	public  void run(){
		 byte[] start= null; 
		 byte[] end = null; 
		
		try{
			ByteArrayOutputStream out = new ByteArrayOutputStream(); 
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeInt(-1); 
			dos.flush(); dos.close();
			start = out.toByteArray(); 
			out = new ByteArrayOutputStream(); 
			dos = new DataOutputStream(out);
			dos.writeInt(-2); 
			dos.flush(); dos.close();
			end = out.toByteArray();
		}catch(Exception e){
			e.printStackTrace(); 
		}
		
		if (verbose){
			System.out.println("Starting thread " + this.id); 
		}
		boolean cond = bombit(end);
		this.end_condition[this.id] = cond;  
		proxy.close();
		
	//	System.out.println("Stats: " +  latency.toString()); 		
	}
	
	/**
	 * @param end
	 */
	long success;
	
	final private boolean bombit(final byte[] end) {
		
		for (long i = 0; i < numFlows + 1000 ; i++){
			if (verbose && (i % interval) == 0 ){
				System.out.println("Thread " + id + " on request: " + i); 
			}
			long tflow_started = System.currentTimeMillis();
			
			try{
				flow.run(proxy);
			}catch(TimeoutException e ){
				return false;
			}
			
			final long total  = System.currentTimeMillis() - tflow_started;  
			try{
				proxy.invokeUnordered(end);
			}catch(Exception e){
				System.out.println( id + "failed request"); 
				continue; 
			}
			end(total, i);
		}
		//System.out.println(latency.getN()/(numFlows*1.0) + "Successfull");
		return true;
	}

	//TODO extract these methods to a standalone package. 
	public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        o.flush(); 
        o.close(); 
        return b.toByteArray();
    }
	
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
	
    WorkloadPerFlow flow; 
	//protected  abstract FlowSimulation chooseNextFlow();
	protected  abstract void end(long t, long i);
}
