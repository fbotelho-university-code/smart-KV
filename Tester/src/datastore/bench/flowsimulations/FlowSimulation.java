package datastore.bench.flowsimulations;

import bftsmart.tom.ServiceProxy;

public interface FlowSimulation {
	
	int WRITE_OP = 0;
	int READ_OP = 0;

	void run(ServiceProxy proxy);

}
