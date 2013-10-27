/**
 * 
 */
package datastore.bench;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.TimeoutException;
import datastore.bench.flowsimulations.FlowSimulation;

/**
 * @author fabiim
 *
 */
public class WorkloadPerFlow {
	public final int[][] ops;
	public final String[] ops_dsc; 
	public final String workload_dsc;
	public final String name; 
	public final String datastoreState; 
	public final byte[][] msgs; 
	/**
	 * @param lsw_0_broadcast_ops
	 * @param lsw_0_brodcast_dsc
	 * @param string
	 */
	public WorkloadPerFlow(String name, int[][] lsw_0_broadcast_ops,
			String[] lsw_0_brodcast_dsc, String string, String datastoreState) {
		this.name = name; 
		this.ops = lsw_0_broadcast_ops; 
		this.ops_dsc = lsw_0_brodcast_dsc; 
		this.workload_dsc = string;
		if (this.ops.length != this.ops_dsc.length){
			throw new RuntimeException("Workload : " + name + ":" + workload_dsc  + "Not good");
		}
		this.datastoreState = datastoreState; 
		
		msgs = new byte[ops.length][];
		for (int i=0; i < ops.length ; i++){
			try{
			ByteArrayOutputStream out = new ByteArrayOutputStream(); 
			DataOutputStream dos = new DataOutputStream(out);
			dos.writeInt(ops[i][2]);
			dos.write(new byte[ops[i][1] -4]);
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
		for (int i=0 ; i < ops.length ; i++){
			if (isWrite(ops[i])){
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
		return ops;
	}
}
