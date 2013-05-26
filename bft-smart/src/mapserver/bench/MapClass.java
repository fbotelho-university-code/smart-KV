package mapserver.bench;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;

public class MapClass extends DefaultSingleRecoverable {
	private static String state = "state"; 
	private ReplicaContext replicaContext;
	private ServiceReplica replica;
	DescriptiveStatistics throughput;
	private boolean measure = false;
	private long interval;
	private long executions;
	private long tpStartTime = System.currentTimeMillis(); 
	
	public static void main(String[] args){
		System.out.println("Usage : prog <id> <throughput measurement interval in operations>");
		int id = Integer.parseInt(args[0]); 
		
		if (args.length ==  2) {
			long interval = Integer.parseInt(args[1]);
			new MapClass(id, interval);
		}
		else{
			new MapClass(id); 
		}
		
		
/*		new MapClass(0,100); 
		new MapClass(1); 
		new MapClass(2);
		new MapClass(3);*/
		
	}
	
	public MapClass(int id) {
          replica = new ServiceReplica(id, this, this);
	}
	
	public MapClass(int i, long interval) {
		replica = new ServiceReplica(i, this, this); 
		this.interval = interval; 
		this.measure = true;
		throughput = new DescriptiveStatistics();
		tpStartTime = System.currentTimeMillis();
		executions =0; 
		Runtime.getRuntime().addShutdownHook( 
	    		new Thread(
	    			new Runnable() {
	    				public void run() {
	    					
	    					System.out.println("RUn shutdown hook: " + throughput.toString()); 
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
		try {
			return execute(command);
		}catch (Exception e){
			e.printStackTrace(); 
		}
		return null; 
	}
	
	private byte[]execute(byte[] command) throws IOException{

		 ByteArrayInputStream in = new ByteArrayInputStream(command);
		 DataInputStream dis = new DataInputStream(in); 
		 int val = dis.readInt();
		 
		 if (val == -2){
			 if (measure){
				 executions++; 
				 if ((executions % interval) == 0 ){
					 throughput.addValue(1000 * interval/ (System.currentTimeMillis() - tpStartTime)); 
					 tpStartTime = System.currentTimeMillis();
				 }
			 }
		 }
		 else{ 
			 return new byte[dis.readInt()];
		 }
		return null; 
	}
}
