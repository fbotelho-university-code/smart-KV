package smartkv.server.bench;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.List;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;

import com.google.common.collect.Lists;

public class MapClass extends DefaultSingleRecoverable {
	public List<TestInformation> tests = Lists.newArrayList(); 
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
	
    
    
	private static String state = "state"; 
	private ReplicaContext replicaContext;
	private ServiceReplica replica;
	DescriptiveStatistics throughput;
	final private boolean measure ;
	 final private long interval;
	 private long executions=0;
	private long tpStartTime = System.currentTimeMillis(); 
	
	public static void main(String[] args){
		int id = Integer.parseInt(args[0]); 
		
		if (args.length ==  2) {
			long interval = Integer.parseInt(args[1]);
			new MapClass(id, interval);
		}
		else{
			new MapClass(id); 
		}
		
		
		/*new MapClass(0,50); 
		new MapClass(1); 
		new MapClass(2);*/
	}
	long ordered=0;
	long unordered=0; 
	
	public MapClass(int id) {
          replica = new ServiceReplica(id, this, this);
          measure = false; 
          interval =0; 
          executions =0; 
	}
	
	public MapClass(int i, long interval) {
		replica = new ServiceReplica(i, this, this); 
		this.interval = interval; 
		this.measure = true;
		throughput = new DescriptiveStatistics();
		tpStartTime = System.nanoTime(); 
		executions =0;

		
		Runtime.getRuntime().addShutdownHook( 
	    		new Thread(
	    			new Runnable() {
	    				public void run() {
	    					SummaryStatistics statsEnd95 = new SummaryStatistics(); 
	    					SummaryStatistics statsEnd99 = new SummaryStatistics(); 
	    					SummaryStatistics statsEnd90 = new SummaryStatistics(); 
	    			        
	    					double[] vals = throughput.getSortedValues();
	    					int len95 = (int) (vals.length *( 0.95));
	    					int len90 = (int) (vals.length *( 0.90));
	    					int len99 = (int) (vals.length *( 0.99));
	    			        
	    					for (int j = vals.length -1 ; j  > (vals.length - len90)  ; j-- ){
	    			        	statsEnd90.addValue(vals[j]);
	    			        }
	    					for (int j = vals.length -1 ; j  > (vals.length - len95)  ; j-- ){
	    			        	statsEnd95.addValue(vals[j]);
	    			        }
	    					
	    					for (int j = vals.length -1 ; j  > (vals.length - len99)  ; j-- ){
	    			        	statsEnd99.addValue(vals[j]);
	    			        }
	    					
	    			        try{
	    			        	if (tests.size() > 0){
	    			        	PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter("./throughput", true)));
	    			        
	    			        	
	    			        	out.println(statsEnd90.getN() + ":" + statsEnd90.getMean() +  ":" + statsEnd90.getStandardDeviation() + ":" + statsEnd90.getMin() + ":" +statsEnd90.getMax() + 
		    			        		( (tests.size() > 0) ?  (":" + tests.get(0).name + ":" + tests.get(0).clients + ":"  + tests.get(0).random) : "") 
		    			        		+ ": " + ordered + ":" + unordered + ":"  
		    			        		+ statsEnd95.getN() + ":" + statsEnd95.getMean() +  ":" + statsEnd95.getStandardDeviation() + ":" + statsEnd95.getMin() + ":" +statsEnd95.getMax() + 
		    			        		+ statsEnd99.getN() + ":" + statsEnd99.getMean() +  ":" + statsEnd99.getStandardDeviation() + ":" + statsEnd99.getMin() + ":" +statsEnd99.getMax()  
	    			        	); 
	    			        	out.flush(); 
	    			        	out.close();
	    			        	}
	    					} catch (IOException e) {
	    						e.printStackTrace(); 
	    					}
	    					
	    				}	
	    			}
	    		)
	    	);

	}

	@Override
	public void setReplicaContext(ReplicaContext replicaContext) {
		this.replicaContext = replicaContext;
	}

	@Override
	public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
		this.unordered++; 
	try{
		return execute(command);
	}catch(Exception e){
		e.printStackTrace(); 
	}
	return null; 
	}

	@Override
	public void installSnapshot(byte[] state) {
		this.state = "state"; 
	}

	@Override
	public byte[] getSnapshot() {
		return state.getBytes(); 
	}

	@Override
	public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
		this.ordered++; 
		
		try {
			return execute(command);
		}catch (Exception e){
			e.printStackTrace(); 
		}
		return null; 
	}
	int warm =0;  
	boolean warmed=false;
	
	private byte[] readNextByteArray(DataInputStream in) throws IOException {
		int size =  in.readInt(); 
		byte[] result = new byte[size];
		in.readFully(result);
		return result; 
	}
	
	
	private byte[]execute(byte[] command) throws IOException{
		 ByteArrayInputStream in = new ByteArrayInputStream(command);
		 DataInputStream dis = new DataInputStream(in); 
		 int val = dis.readInt();
		 
		 if (!warmed){
			 if (warm++ == 5000) {
				 warmed = true;
				 tpStartTime = System.nanoTime(); 
			 }
		 }
		 
		 
		 if (val > 0  ){
			 return new byte[dis.readInt()];
		 }
		 else if (val == -2){
			 if (measure && warmed){
				 executions++;
				 if ((executions % interval) == 0 ){
					 executions = 0; 
					 try{
						 throughput.addValue(1000 * 1000000 * interval/ (System.nanoTime() - tpStartTime));
						 tpStartTime = System.nanoTime();
					 }catch (ArithmeticException e){
						System.err.println("Atirhtmetic exception") ; 
					 }
				 }
			 }
		 }else if (val == -3){
			 //End of test keep data
			 try{
				 TestInformation t = (TestInformation) deserialize(readNextByteArray(dis));
				 tests.add(t); 
			 }catch(Exception e){e.printStackTrace(); }
		 }
		return null; 
	}
}
