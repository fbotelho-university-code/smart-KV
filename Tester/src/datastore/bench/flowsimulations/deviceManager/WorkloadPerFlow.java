package datastore.bench.flowsimulations.deviceManager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.TimeoutException;
import datastore.bench.flowsimulations.FlowSimulation;

public class WorkloadPerFlow {
	
	public final int[][] requests;
	public final String[] requestDescription;
	public final String workloadDescription;
	public final byte[][] msgs ; 

	public WorkloadPerFlow(int[][] val, String[] requestDescription , String workloadDescription){
		requests = val; 
		this.requestDescription = requestDescription; 
		this.workloadDescription = workloadDescription; 
		msgs = new byte[requests.length][];
		for (int i=0; i < requests.length ; i++){
			try{
			ByteArrayOutputStream out = new ByteArrayOutputStream(); 
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeInt(requests[i][2]);
			dos.write(new byte[requests[i][1] -4]);
			dos.flush(); 
			dos.close();
			msgs[i] = out.toByteArray();
			}catch(Exception e){
				e.printStackTrace(); 
				System.exit(-1);
			}
			
		}
	}
	
	public void run(ServiceProxy proxy) throws TimeoutException {
		for (int i=0 ; i < requests.length ; i++){
			if (isWrite(requests[i])){
				proxy.invokeOrdered(msgs[i]); 
			}
			else{
				proxy.invokeUnordered(msgs[i]); 
			}
		}
	}
	
	private boolean isWrite(int[] is) {
		return is[0] == FlowSimulation.WRITE_OP; 
	}
	
	public int[][] getFlows() {
		// TODO Auto-generated method stub
		return requests;
	}

}
