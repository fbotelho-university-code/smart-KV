package datastore.bench;

import java.util.Random;

import datastore.bench.flowsimulations.FlowSimulation;
import datastore.bench.flowsimulations.deviceManager.DeviceManager;


public class MultiFlowTypes extends BenchClient{
	FlowSimulation newPing = new DeviceManager(DeviceManager.NewPingingOneExistentIn1000H); 
	FlowSimulation existentPing = new DeviceManager(DeviceManager.PingKnown1000H); 
	Random generator = new Random(); 
	public MultiFlowTypes(int id, int numberOfFlows,boolean verbose, long interval, boolean storedStatistics, int start_id) {
		super(id, numberOfFlows, verbose,interval, storedStatistics, start_id);
		
	}

	@Override
	protected FlowSimulation chooseNextFlow() {
		return newPing;  
		/*if (generator.nextBoolean()){
			return newPing; 
		}
		return existentPing;*/ 
	}

}
