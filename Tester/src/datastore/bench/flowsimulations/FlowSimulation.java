package datastore.bench.flowsimulations;

import bftsmart.tom.ServiceProxy;
import bftsmart.tom.TimeoutException;

public interface FlowSimulation {
	
	int WRITE_OP = 1;
	int READ_OP = 0;
	
	
	
	void run(ServiceProxy proxy) throws TimeoutException;
	
	public int[][] getFlows();
	
	

}
