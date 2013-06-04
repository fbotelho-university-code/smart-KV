package datastore.bench.flowsimulations.deviceManager;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import bftsmart.tom.ServiceProxy;
import datastore.bench.flowsimulations.FlowSimulation;

public class DeviceManager implements FlowSimulation{
	public static int[][] hosts500 = {
		{ FlowSimulation.READ_OP,406,82},
		{ FlowSimulation.READ_OP,28,1758},
		{ FlowSimulation.WRITE_OP,1786,1},
		{ FlowSimulation.READ_OP,406,82},
		{ FlowSimulation.READ_OP,28,1680},
	};
	
	int[][] requests; 
	byte[][] msgs ; 
	
	public DeviceManager(int[][] val){
		requests = val; 
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
	
	public static final int[][] NewPingingOneExistentIn1000H= {
		{ FlowSimulation.READ_OP,127,1},
		{ FlowSimulation.READ_OP,50,0},
		{ FlowSimulation.READ_OP,127,1},
		{ FlowSimulation.READ_OP,50,77},
		{ FlowSimulation.READ_OP,127,1},
		{ FlowSimulation.WRITE_OP,50,77},
	}; 
	
/*			{ FlowSimulation.READ_OP,406,0},
			{ FlowSimulation.READ_OP,50,465124},
			{ FlowSimulation.READ_OP,16,1703264},
			{ FlowSimulation.READ_OP,25,1},
			{ FlowSimulation.READ_OP,63,82},
			{ FlowSimulation.WRITE_OP,231,1},
			{ FlowSimulation.WRITE_OP,1699,1},
			{ FlowSimulation.WRITE_OP,488,0},
			{ FlowSimulation.READ_OP,50,465124},
			{ FlowSimulation.READ_OP,25,231},
			{ FlowSimulation.READ_OP,50,465124},
			{ FlowSimulation.WRITE_OP,465174,1},
			{ FlowSimulation.WRITE_OP,1699,1},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1671},
			{ FlowSimulation.WRITE_OP,3449,1},
			{ FlowSimulation.WRITE_OP,488,82},
			{ FlowSimulation.WRITE_OP,488,82},
			{ FlowSimulation.READ_OP,50,465124},
			{ FlowSimulation.READ_OP,25,231},
			{ FlowSimulation.READ_OP,50,465124},
			{ FlowSimulation.WRITE_OP,465638,1},
			{ FlowSimulation.WRITE_OP,1774,1},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1746},
			{ FlowSimulation.WRITE_OP,1786,1},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1758},
			{ FlowSimulation.WRITE_OP,1774,1},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1758},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1746},
			{ FlowSimulation.WRITE_OP,1786,1},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1758},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1758},
			{ FlowSimulation.WRITE_OP,1798,1},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1758},
			{ FlowSimulation.READ_OP,406,82},
			{ FlowSimulation.READ_OP,28,1770},
			{ FlowSimulation.WRITE_OP,1798,1},	
	}; 
	*/
	public static final int [][] PingKnown1000H = {
		{ FlowSimulation.READ_OP,406,82},
		{ FlowSimulation.READ_OP,28,1770},
		{ FlowSimulation.WRITE_OP,1786,1},
		{ FlowSimulation.READ_OP,406,82},
		{ FlowSimulation.READ_OP,28,1680},
		{ FlowSimulation.READ_OP,406,82},
		{ FlowSimulation.READ_OP,28,1680},
		{ FlowSimulation.WRITE_OP,3458,1},
		{ FlowSimulation.WRITE_OP,488,82},
		{ FlowSimulation.WRITE_OP,488,82},
		{ FlowSimulation.READ_OP,50,465124},
		{ FlowSimulation.READ_OP,25,231},
		{ FlowSimulation.READ_OP,50,465124},
		{ FlowSimulation.WRITE_OP,465174,1},
		{ FlowSimulation.WRITE_OP,1774,1},
		{ FlowSimulation.READ_OP,406,82},
		{ FlowSimulation.READ_OP,28,1758},
	}; 
	
	
	@Override
	public void run(ServiceProxy proxy) {
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
}
