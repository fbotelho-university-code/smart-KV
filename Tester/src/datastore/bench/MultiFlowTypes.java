package datastore.bench;

import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.collect.Maps;

import datastore.bench.flowsimulations.FlowSimulation;
import datastore.bench.flowsimulations.deviceManager.WorkloadPerFlow;

/*
class OpenFlowMessageWorkload{
	public final FlowSimulation flows;
	public final String[] flows_descriptions; 
	public final int max;
	public String description; 

	public OpenFlowMessageWorkload(FlowSimulation[] flows, String desc, String[] flows_descriptions){
		this.flows = flows; 
		this.max = flows.length; 
		this.description = desc; 
		this.flows_descriptions = flows_descriptions; 
	}
}
*/
public class MultiFlowTypes extends BenchClient{
	
	static class LearningSwitch{
		//LSW-0 : Original (with java serialization)   
		public static int[][] lsw_0_broadcast_ops = { 
			{ FlowSimulation.WRITE_OP,113,1}, 
		};
		public static String[] lsw_0_brodcast_dsc = { 
			"Write source address in ingress switch-table", 
		};
		
		public static int[][] lsw_0_unicast_ops = {// ARP REPLY 
			{ FlowSimulation.WRITE_OP,113,1}, //Write arp reply source
			{ FlowSimulation.READ_OP,36,77}, //Read destiny device 
		};
		public static String[] lsw_0_unicast_dsc = { 
			"Write source address in ingress switch-table",
			"Read egress port for destination in ingress switch-table",
		};
		
		
		//LSW-1 : Original   
		public static int[][] lsw_1_broadcast_ops = { 
			{ FlowSimulation.WRITE_OP,29,1}, 
		};
		public static String[] lsw_1_dsc = { 
			"Write source address in ingress switch-table", 
		};
		
		public static int[][] lsw_1_unicast_ops = {// ARP REPLY 
			{ FlowSimulation.WRITE_OP,29,1}, //Write arp reply source
			{ FlowSimulation.READ_OP,27,6}, //Read destiny device 
		};
		public static String[] lsw_1_unicast_dsc = { 
			"Write source address in ingress switch-table",
			"Read egress port for destination in ingress switch-table",
		};
	}
	
	static class LoadBalancerNew{
		public static String read_Vip_key = "Read the VIP id for the destination IP";  
		public static String obtain_Vip_info = "Read the VIP Information"; 
		public static String read_pool = "Read the choosen Pool for this request";
		public static String cond_update_pool = "Conditional replace pool after round-robin changes";
		public static String read_member = "Read the chosen Member"; 
		
		public static int[][] lbw_1_arp_request = {
			{ FlowSimulation.READ_OP,104,12},
			{ FlowSimulation.READ_OP,29,513},
		};
		
		public static String[] lbw_1_arp_request_dsc ={
			LoadBalancerNew.read_Vip_key, 
			obtain_Vip_info + " (Proxy MAC address)",
			 
		};
		
		public static int[][] lbw_1_ip_packet = {
			{ FlowSimulation.READ_OP,104,12},
			{ FlowSimulation.READ_OP,29,513},
			{ FlowSimulation.READ_OP,30,373},
			{ FlowSimulation.WRITE_OP,772,1},
			{ FlowSimulation.READ_OP,32,225},
			
		}; 
		
		public static String[] lbw_1_ip_packet_dsc = {
			LoadBalancerNew.read_Vip_key, 
			LoadBalancerNew.obtain_Vip_info + " ( what pool to use) ",
			read_pool, 
			cond_update_pool,
			read_member,
		}; 
		
		public static int[][] lbw_1_normal_packet = {
			{ FlowSimulation.READ_OP,104,0},
		};
		public static String[] lbw_1_normal_packet_dsc = {
			read_Vip_key
		};
	}
	static class LoadBalancer{
		public static int[][] lbw_1 ={
			{ FlowSimulation.READ_OP,106,509},
			{ FlowSimulation.READ_OP,26,369},
			{ FlowSimulation.WRITE_OP,395,1},
			{ FlowSimulation.READ_OP,28,221},
		};
		
		public static String[] lbw_1_dsc ={
			"Read the VIP for the destination IP", 
			"Read the chosen Pool for this request",
			"Update the Pool after round-robin update", 
			"Read the chosen Member", 
		};
		
		public static int[][] lbw_2 ={
			{ FlowSimulation.READ_OP,106,0},
		};
		
		public static String[] lbw_2_dsc ={
			"Read the VIP for the destination IP", 
		};
		
		public static int[][] lbw_3 ={
			{ FlowSimulation.READ_OP,106,509},
			{ FlowSimulation.READ_OP,18,509},
		};
		
		public static String[] lbw_3_dsc ={
			"Read the VIP for the destination IP", 
			"Read the MAC address of the VIP",
		};
		//Optimizations 
		
		public static int[][] lbw_1_1 ={
			{ FlowSimulation.READ_OP,26,369},
			{ FlowSimulation.WRITE_OP,395,1},
		};
		public static String[] lbw_1_1_dsc ={
			"Read the chosen Pool for this request",
			"Update the Pool after round-robin update", 
		};
		
		public static int[][] lbw_1_2 ={
			{ FlowSimulation.WRITE_OP,26, 221},
		};
		public static String[] lbw_1_2_dsc = {
			"Get next member",
		};
		
	}
	
	static class DeviceManager{
		
		public static int[][] dmw_1 = {
			{ FlowSimulation.READ_OP,406,1680},
			{ FlowSimulation.WRITE_OP,3458,0},
			//{ FlowSimulation.WRITE_OP,1302,1},
			{ FlowSimulation.READ_OP,406,1680},
		};
		public static String[] dmw_1_dsc ={
				"Read the source device",
				"Update \"last seen\" timestamp",
				"Read the destination device", 
		};
		
		public static int[][] dmw_2 ={
			{ FlowSimulation.READ_OP,406,0},
			{ FlowSimulation.WRITE_OP,56,82},
			{ FlowSimulation.WRITE_OP,1708,1},
			{ FlowSimulation.WRITE_OP,2086,0},
			{ FlowSimulation.READ_OP,390,0},
			{ FlowSimulation.WRITE_OP,521,0},
			{ FlowSimulation.READ_OP,406,1680},
		};
		
		public static String[] dmw_2_dsc ={
			"1) Read the source device", 
			"2) Get and increment the device id counter",
			"3) Put new device in device table",
			"4) Put new device in \\texttt{(MAC,VLAN)} table",
			"5) Get devices with source IP",
			"6) Update devices with source IP",
			"7) Read the destination device",
		};
	}
	//FlowSimulation newPing = new DeviceManager(DeviceManager.NewPingingOneExistentIn1000H); 
	//FlowSimulation existentPing = new DeviceManager(DeviceManager.PingKnown1000H);
	int index=0;
	public static HashMap<String, WorkloadPerFlow> simulations = Maps.newHashMap(); 

	static{
		//Learning Switch 
		simulations.put("lsw-1", new WorkloadPerFlow(LearningSwitch.lsw_1_ops, LearningSwitch.lsw_1_dsc, "Broadcast Packet"));  
		simulations.put("lsw-2", new WorkloadPerFlow(LearningSwitch.lsw_2_ops, LearningSwitch.lsw_2_dsc, "Unicast Packet"));  
		
		simulations.put("lsw-1-1", new WorkloadPerFlow(LearningSwitch.lsw_1_1_ops, LearningSwitch.lsw_1_dsc, "Broadcast Packet - No Serialization"));  
		simulations.put("lsw-2-1", new WorkloadPerFlow(LearningSwitch.lsw_2_1_ops, LearningSwitch.lsw_2_dsc, "Unicast Packet - No Serialization"));  

		//Load Balancer 
		simulations.put("lbw-1", new WorkloadPerFlow(LoadBalancer.lbw_1, LoadBalancer.lbw_1_dsc, "Packet to a VIP"));  
		simulations.put("lbw-1-1", new WorkloadPerFlow(LoadBalancer.lbw_1_1, LoadBalancer.lbw_1_1_dsc, "Packet to a VIP: Cache"));
		simulations.put("lbw-1-2", new WorkloadPerFlow(LoadBalancer.lbw_1_2, LoadBalancer.lbw_1_2_dsc, "Packet to a VIP: Middleware Approach"));  
		simulations.put("lbw-2", new WorkloadPerFlow(LoadBalancer.lbw_2, LoadBalancer.lbw_2_dsc, "Normal packet"));  
		simulations.put("lbw-3", new WorkloadPerFlow(LoadBalancer.lbw_3, LoadBalancer.lbw_3_dsc, "ARP Request for a VIP"));
		
		//Device Manager 
		simulations.put("dmw-1", new WorkloadPerFlow(DeviceManager.dmw_1, DeviceManager.dmw_1_dsc, "Known devices"));  
		simulations.put("dmw-2", new WorkloadPerFlow(DeviceManager.dmw_2, DeviceManager.dmw_2_dsc, "Unknown source"));  

		
/*		simulations.put("lsw-2", new OpenFlowMessageWorkload(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.worstCase1),
				}, "Unicast Packet"));
		//
		simulations.put("w2-0",new OpenFlowMessageWorkload(new FlowSimulation[] {
				new LoadBalancer(LoadBalancer.lb0),
				}, "Arp Request to VIP"));
		simulations.put("w2-1",new OpenFlowMessageWorkload(new FlowSimulation[] {
				new LoadBalancer(LoadBalancer.lb1),
				}, "Unicast packet to VIP"));
		simulations.put("w2-2",new OpenFlowMessageWorkload(new FlowSimulation[] {
				new LoadBalancer(LoadBalancer.lb2)}, "Packet to IP"));
		
		//
		simulations.put("w3", new OpenFlowMessageWorkload(new FlowSimulation[] { 
				//new DeviceManager(DeviceManager.bestCase0),
				new DeviceManager(DeviceManager.bestCase1) }, "Device Manager best case"));
		//
		simulations.put("w4-0", new OpenFlowMessageWorkload(new FlowSimulation[] { 
				new DeviceManager(DeviceManager.worstCase0),
				}, "Device manager worst case"));
		simulations.put("w4-1", new OpenFlowMessageWorkload(new FlowSimulation[] { 
				new DeviceManager(DeviceManager.worstCase1),
				}, "Device manager worst case"));
		simulations.put("w4-2", new OpenFlowMessageWorkload(new FlowSimulation[] { 
				new DeviceManager(DeviceManager.worstCase2)}, "Device manager worst case"));
		//
		
		simulations.put("devOneRead", new OpenFlowMessageWorkload(new FlowSimulation[] { 
				new DeviceManager(DeviceManager.bestCaseOneRead0),
				new DeviceManager(DeviceManager.bestCaseOneRead1) }, "Device Manager best case with one read"));
		
			simulations.put("leanCacheReadsOnly", new OpenFlowMessageWorkload(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.noOptimalCache0),
				new LearningSwitch(LearningSwitch.noOptimalCache1),
				new LearningSwitch(LearningSwitch.noOptimalCache2)}, "Learning Switch with Cache fors reads only")); 
		
		
		simulations.put("learnCache", new OpenFlowMessageWorkload(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.cachedCase0),
				new LearningSwitch(LearningSwitch.cachedCase1),
				new LearningSwitch(LearningSwitch.cachedCase2)}, "Learning Switch with Cache")); 
	*/	
	}

	/*static{
		simulations.put("base", new FlowSimula(new FlowSimulation[] { 
				new DeviceManager(DeviceManager.geneCase)}, "Base case to find is measuring is right"));
		simulations.put("devBest0", new FlowSimula(new FlowSimulation[] { 
				new DeviceManager(DeviceManager.bestCase0),
				new DeviceManager(DeviceManager.bestCase1) }, "Device Manager best case"));
		simulations.put("devOneRead", new FlowSimula(new FlowSimulation[] { 
				new DeviceManager(DeviceManager.bestCaseOneRead0),
				new DeviceManager(DeviceManager.bestCaseOneRead1) }, "Device Manager best case with one read"));
		simulations.put("devWorst", new FlowSimula(new FlowSimulation[] { 
					new DeviceManager(DeviceManager.worstCase0),
					new DeviceManager(DeviceManager.worstCase1),
					new DeviceManager(DeviceManager.worstCase2)}, "Device manager worst case"));
		simulations.put("w1-0", new FlowSimula(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.worstCase0),
				new LearningSwitch(LearningSwitch.worstCase1),
				new LearningSwitch(LearningSwitch.worstCase2)}, "Learning Switch real Worst case"));
		simulations.put("w1-1", new FlowSimula(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.worstCase0),
				new LearningSwitch(LearningSwitch.worstCase1),
				new LearningSwitch(LearningSwitch.worstCase2)}, "Learning Switch real Worst case"));
		simulations.put("w1-2", new FlowSimula(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.worstCase0),
				new LearningSwitch(LearningSwitch.worstCase1),
				new LearningSwitch(LearningSwitch.worstCase2)}, "Learning Switch real Worst case"));
		simulations.put("learnCache", new FlowSimula(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.cachedCase0),
				new LearningSwitch(LearningSwitch.cachedCase1),
				new LearningSwitch(LearningSwitch.cachedCase2)}, "Learning Switch with Cache")); 
		simulations.put("leanCacheReadsOnly", new FlowSimula(new FlowSimulation[] {
				new LearningSwitch(LearningSwitch.noOptimalCache0),
				new LearningSwitch(LearningSwitch.noOptimalCache1),
				new LearningSwitch(LearningSwitch.noOptimalCache2)}, "Learning Switch with Cache fors reads only")); 
		simulations.put("lb",new FlowSimula(new FlowSimulation[] {
						new LearningSwitch(LoadBalancer.lb0),
						new LearningSwitch(LoadBalancer.lb1),
						new LearningSwitch(LoadBalancer.lb2)}, "Learning Switch with Cache fors reads only"));
				
	}*/
	
	public static String getValues(){
		StringBuilder s = new StringBuilder();
		for (Entry<String, WorkloadPerFlow> en : simulations.entrySet()){
			s.append(en.getKey() + " - " + en.getValue().workloadDescription + "\n");
		}
		return s.toString(); 
	}
	
		
	public MultiFlowTypes(int id, int numberOfFlows,boolean verbose, long interval, boolean storedStatistics, int start_id, String type, DescriptiveStatistics stats, Boolean[] end_condition) {
		super(id, numberOfFlows, verbose,interval, storedStatistics, start_id,  type, stats, end_condition);
		flow = MultiFlowTypes.simulations.get(type);
	}
	
	/*
	protected FlowSimulation chooseNextFlow() {
		return flows[index++ % max]; 
	}*/
	
	@Override
	protected void end(long total, long i){
		if (i > 1000){
			latency.addValue(total);
		}
	}
}
