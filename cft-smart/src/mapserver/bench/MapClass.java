package mapserver.bench;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;

public class MapClass extends DefaultSingleRecoverable {

	private ReplicaContext replicaContext;
	private ServiceReplica replica;
	DescriptiveStatistics throughput;
	private boolean measure = false;
	private long interval;
	private long executions;
	private long tpStartTime; 
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
	}

	@Override
	public void setReplicaContext(ReplicaContext replicaContext) {
		this.replicaContext = replicaContext;
	}

	@Override
	public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
		return execute(command);
	}

	@Override
	public void installSnapshot(byte[] state) {
		
	}

	@Override
	public byte[] getSnapshot() {
		return new byte[1]; 
	}

	@Override
	public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
		return execute(command);
	}
	private  long counter =0; 
	private byte[]execute(byte[] command){
		return null; 
		/*counter++; 
		if ((counter % 1000) == 0) 
			System.out.println( counter); 
		return null; 
		*/
		/*
		try{
		 ByteArrayInputStream in = new ByteArrayInputStream(command);
		 DataInputStream dis = new DataInputStream(in); 
		 int val = dis.readInt(); 
		 
		 if (val == -2){
			 executions++; 
			 //ending new flow
			 if (measure){
					if ((executions % interval) == 0 ){
						System.out.println("Executions : " + executions + " Interval: " + interval + " exec%int " + (executions % interval)); 
						throughput.addValue(1000 * interval/ (System.currentTimeMillis() - tpStartTime)); 
						tpStartTime = System.currentTimeMillis();
						System.out.println(throughput.toString());
					}
				}
		 }
		 else{ 
			 return new byte[dis.readInt()];
		 }
		}catch(Exception e){
			e.printStackTrace(); 
		}
		return null; */
	}
}
