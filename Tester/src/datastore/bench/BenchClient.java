package datastore.bench;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import bftsmart.tom.ServiceProxy;
import datastore.bench.flowsimulations.FlowSimulation;


public abstract class BenchClient implements Runnable{
	
	public static void main(String[] args) throws InterruptedException{
		if (args.length < 3) {
            System.out.println("Usage: ... ThroughputLatencyClient <num. threads> <number of operations per thread> <interval> <verbose?> <stored statistics?> <start_id>"); 
            System.exit(-1);
        }
		
	    int numThreads = Integer.parseInt(args[0]);
        int numberOfFlows = Integer.parseInt(args[1]); 
        boolean verbose =Boolean.parseBoolean(args[3]);
        long interval = Integer.parseInt(args[2]); 
        boolean storedStatistics = Boolean.parseBoolean(args[4]); 
        int start_id = Integer.parseInt(args[5]); 
        
        Thread[] c = new Thread[numThreads];
        
        if (verbose){
        	System.out.println("Creating threads"); 
        }
        
        for(int i=0; i<numThreads; i++) {
            c[i] = new Thread(new MultiFlowTypes(i, numberOfFlows, verbose, interval, storedStatistics, start_id)); 
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
        System.exit(0);
	}

	protected final ServiceProxy proxy ; 
	protected  final int id;
	private final DescriptiveStatistics latency = new DescriptiveStatistics();
	private final int numFlows;
	private final boolean verbose;  
	private final long interval;
	private final boolean storedStatistics;
	private final  int start_id;
	private SummaryStatistics latency2 = new SummaryStatistics(); 
	
	public BenchClient(int id, int numFlows, boolean verbose, long interval, boolean storedStatistics, int start_id){
		this.id = id; 
		proxy = new ServiceProxy(start_id + id); 
		this.numFlows = numFlows; 
		this.verbose = verbose; 
		this.interval = interval;
		this.storedStatistics = storedStatistics;
		this.start_id = start_id; 
		if (verbose)
			System.out.println("Starting thread :" + id); 
	}
	
	@Override
	public  void run(){
		Runtime.getRuntime().addShutdownHook( 
	    		new Thread(
	    			new Runnable() {
	    				public void run() {
	    					System.out.println("RUn shutdown hook: " + (storedStatistics ? latency.toString() : latency2.toString() )); 
	    				}	
	    			}
	    		)
	    	);
		
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
		bombit(end);
		System.out.println("Stats: " + (storedStatistics ? latency.toString() : latency2.toString() ));
		
	}

	/**
	 * @param end
	 */
	private void bombit(final byte[] end) {
		for (long i = 0; i < numFlows + 1000 ; i++){
			if (verbose && (i % interval) == 0 ){
				System.out.println("Thread " + id + " on request: " + i); 
			}
			final long tflow_started = System.currentTimeMillis(); 
			FlowSimulation chooseFlow = chooseNextFlow();
			chooseFlow.run(proxy);	 
			final long total  = System.currentTimeMillis() - tflow_started;  
			proxy.invokeUnordered(end);
			if (storedStatistics && i > 1000) {
				latency.addValue(total);
			}
			else if (i >1000){
				latency2 .addValue(total); 
			}
		}
	}
	protected  abstract FlowSimulation chooseNextFlow();
}
