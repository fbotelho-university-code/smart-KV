package datastore.bench;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map.Entry;

import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

import com.google.common.collect.Maps;

import datastore.bench.flowsimulations.FlowSimulation;

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
	public static HashMap<String, WorkloadPerFlow> simulations = Maps.newHashMap();
	
	static{
		simulations.put("lsw-0-broadcast", new WorkloadPerFlow("lsw-0" , LearningSwitch.lsw_0_broadcast_ops, LearningSwitch.lsw_0_brodcast_dsc, "Broadcast Packet", "Original"));
		simulations.put("lsw-0-unicast", new WorkloadPerFlow("lsw-0" , LearningSwitch.lsw_0_unicast_ops, LearningSwitch.lsw_0_unicast_dsc, "Unicast Packet", "Original"));
		//Serialization 
		simulations.put("lsw-1-unicast", new WorkloadPerFlow("lsw-1" , LearningSwitch.lsw_1_unicast_ops, LearningSwitch.lsw_1_unicast_dsc, "Unicast Packet", "Manual Serialization"));
		simulations.put("lsw-1-broadcast", new WorkloadPerFlow("lsw-1" , LearningSwitch.lsw_1_broadcast_ops, LearningSwitch.lsw_1_broadcast_dsc, "Broadcast Packet", "Manual Serialization"));
	}
	
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
		public static String[] lsw_1_broadcast_dsc = { 
			"Write source address in ingress switch-table", 
		};
		
		public static int[][] lsw_1_unicast_ops = {// ARP REPLY 
			{ FlowSimulation.WRITE_OP,29,1}, //Write arp reply source
			{ FlowSimulation.READ_OP,27,6}, //Read destiny device 
		};
		public static String[] lsw_1_unicast_dsc = { 
			"Associate source address to ingress port",
			"Read egress port for destination address", 
		};
	}
	
	
	static{
		simulations.put("lbw-0-arp-request", new WorkloadPerFlow("lbw-0" , LoadBalancerNew.lbw_0_arp_request, LoadBalancerNew.lbw_0_arp_request_dsc, "Arp Request to a VIP", "Original"));
		simulations.put("lbw-0-ip-to-vip", new WorkloadPerFlow("lbw-0" , LoadBalancerNew.lbw_0_ip_packet, LoadBalancerNew.lbw_0_ip_packet_dsc, "IP packet to a VIP", "Original"));
		simulations.put("lbw-0-ip-to-notvip", new WorkloadPerFlow("lbw-0" , LoadBalancerNew.lbw_0_normal_packet, LoadBalancerNew.lbw_0_normal_packet_dsc, "Normal Packet", "Original"));
		// Cross References 
		simulations.put("lbw-1-arp-request", new WorkloadPerFlow("lbw-1" , LoadBalancerNew.lbw_1_arp_request, LoadBalancerNew.lbw_1_arp_request_dsc, "Arp Request to a VIP", "Cross Reference Tables"));
		simulations.put("lbw-1-ip-to-vip", new WorkloadPerFlow("lbw-1" , LoadBalancerNew.lbw_1_ip_packet, LoadBalancerNew.lbw_1_ip_packet_dsc, "IP packet to a VIP", "Cross Reference Tables"));
		simulations.put("lbw-1-ip-to-notvip", new WorkloadPerFlow("lbw-1" , LoadBalancerNew.lbw_1_normal_packet, LoadBalancerNew.lbw_1_normal_packet_dsc, "Normal Packet", "Cross Reference Tables"));
		// Use replace with timestamp 
//		simulations.put("lbw-2-arp-request", new WorkloadPerFlow("lbw-2" , LoadBalancerNew.lbw_2_arp_request, LoadBalancerNew.lbw_2_arp_request_dsc, "Arp Request to a VIP", "Timestamp Values"));
		simulations.put("lbw-2-ip-to-vip", new WorkloadPerFlow("lbw-2" , LoadBalancerNew.lbw_2_ip_packet, LoadBalancerNew.lbw_2_ip_packet_dsc, "IP packet to a VIP", "Timestamp Values"));
//		simulations.put("lbw-2-ip-to-notvip", new WorkloadPerFlow("lbw-2" , LoadBalancerNew.lbw_2_normal_packet, LoadBalancerNew.lbw_2_normal_packet_dsc, "Normal Packet", "Timestamp Values"));
		// Columns
		simulations.put("lbw-3-arp-request", new WorkloadPerFlow("lbw-3" , LoadBalancerNew.lbw_3_arp_request, LoadBalancerNew.lbw_3_arp_request_dsc, "Arp Request to a VIP", "Columns"));
		simulations.put("lbw-3-ip-to-vip", new WorkloadPerFlow("lbw-3" , LoadBalancerNew.lbw_3_ip_packet, LoadBalancerNew.lbw_3_ip_packet_dsc, "IP packet to a VIP", "Columns"));
		simulations.put("lbw-3-ip-to-notvip", new WorkloadPerFlow("lbw-3" , LoadBalancerNew.lbw_3_normal_packet, LoadBalancerNew.lbw_3_normal_packet_dsc, "Normal Packet", "Columns"));
		// Micro Componenets
		simulations.put("lbw-4-ip-to-vip", new WorkloadPerFlow("lbw-4" , LoadBalancerNew.lbw_4_ip_packet, LoadBalancerNew.lbw_4_ip_packet_dsc, "IP packet to a VIP", "Micro Components"));
	}
	
	static class LoadBalancerNew{
		public static String read_Vip_key = "Read the VIP id for the destination IP";  
		public static String obtain_Vip_info = "Read the VIP Information"; 
		public static String obtain_Vip_info_pool = "Read the VIP Information (pool information)"; 
		public static String obtain_Vip_info_mac  = "Read the VIP Information (Proxy MAC address)"; 
		public static String read_pool = "Read the choosen Pool for this request";
		public static String cond_update_pool = "Conditional replace pool after round-robin changes";
		public static String read_member = "Read the chosen Member"; 
		
		public static int[][] lbw_0_arp_request = {
			{ FlowSimulation.READ_OP,104,8},
			{ FlowSimulation.READ_OP,29,509},
		};
		
		public static String[] lbw_0_arp_request_dsc ={
			LoadBalancerNew.read_Vip_key, 
			obtain_Vip_info_mac
			 
		};
		
		public static int[][] lbw_0_ip_packet = {
			{ FlowSimulation.READ_OP,104,8},
			{ FlowSimulation.READ_OP,29,509},
			{ FlowSimulation.READ_OP,30,369},
			{ FlowSimulation.WRITE_OP,772,1},
			{ FlowSimulation.READ_OP,32,221},
		};
		
		public static String[] lbw_0_ip_packet_dsc = {
			LoadBalancerNew.read_Vip_key, 
			LoadBalancerNew.obtain_Vip_info_pool,
			read_pool, 
			cond_update_pool,
			read_member,
		}; 
		
		public static int[][] lbw_0_normal_packet = {
			{ FlowSimulation.READ_OP,104,0},
		};
		
		public static String[] lbw_0_normal_packet_dsc = {
			read_Vip_key
		};
		
		// LBW1 - Cross references eliminate the trouble of reading id and reading VIP (first 2 requests).
		public static int[][] lbw_1_arp_request = {
			{ FlowSimulation.READ_OP,104,509},
		};
		
		public static String[] lbw_1_arp_request_dsc ={
			obtain_Vip_info_mac,
		};
		
		public static int[][] lbw_1_ip_packet = {
			{ FlowSimulation.READ_OP,104,509},
			{ FlowSimulation.READ_OP,30,369},
			{ FlowSimulation.WRITE_OP,772,1},
			{ FlowSimulation.READ_OP,32,221},
		};
		
		public static String[] lbw_1_ip_packet_dsc = {
			LoadBalancerNew.obtain_Vip_info_pool,
			read_pool, 
			cond_update_pool,
			read_member,
		}; 
		
		public static int[][] lbw_1_normal_packet = {
			{ FlowSimulation.READ_OP,104,0},
		};
		
		public static String[] lbw_1_normal_packet_dsc = {
			obtain_Vip_info_pool
		};
	
		// Replace replace by replace with timestamp. 

		public static int[][] lbw_2_ip_packet = {
			{ FlowSimulation.READ_OP,104, 513},
			{ FlowSimulation.READ_OP,30,373},
			{ FlowSimulation.WRITE_OP,403,1},
			{ FlowSimulation.READ_OP,32,225},
		};
		
		public static String[] lbw_2_ip_packet_dsc = {
			LoadBalancerNew.obtain_Vip_info_pool,
			read_pool, 
			cond_update_pool,
			read_member,
		}; 
		
		// Column values  
		
		public static int[][] lbw_3_arp_request = {
			{ FlowSimulation.READ_OP,62,324},
		};
		
		public static String[] lbw_3_arp_request_dsc ={
			obtain_Vip_info_mac,
		};
		
		public static int[][] lbw_3_ip_packet = {
			{ FlowSimulation.READ_OP,62, 324},
			{ FlowSimulation.READ_OP,30,373},
			{ FlowSimulation.WRITE_OP,403,1},
			{ FlowSimulation.READ_OP,44,4},
		};
		
		public static String[] lbw_3_ip_packet_dsc = {
			LoadBalancerNew.obtain_Vip_info_pool,
			read_pool, 
			cond_update_pool,
			read_member,
		}; 
		
		public static int[][] lbw_3_normal_packet = {
			{ FlowSimulation.READ_OP,62,0},
		};
		
		public static String[] lbw_3_normal_packet_dsc = {
			obtain_Vip_info_pool
		};
		
		//Micro componenets 
		public static int[][] lbw_4_ip_packet = {
			{ FlowSimulation.READ_OP,62, 324},
			{ FlowSimulation.WRITE_OP,11,4},
		};
		
		public static String[] lbw_4_ip_packet_dsc = {
			LoadBalancerNew.obtain_Vip_info_pool,
			"Round robin pool and return member address",
		};
	}
	
	/*
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
	*/
	static{
		//Original
		simulations.put("dm-0-unknown", new WorkloadPerFlow("dm-0" , DeviceManagerNew.dmw_0_arp_for_unknown, DeviceManagerNew.dmw_0_arp_for_unknown_dsc, "ARP from Unknown Source", "Original"));
		simulations.put("dm-0-known", new WorkloadPerFlow("dm-0" , DeviceManagerNew.dmw_0_ip_for_known, DeviceManagerNew.dmw_0_ip_for_known_dsc, "Known Devices", "Original"));
		// Cross Reference And Improved Serialization 
		simulations.put("dm-1-unknown", new WorkloadPerFlow("dm-1" , DeviceManagerNew.dmw_1_arp_for_unknown, DeviceManagerNew.dmw_1_arp_for_unknown_dsc, "ARP from Unknown Source", "Cross Reference Tables \\& improved Serialization"));
		simulations.put("dm-1-known", new WorkloadPerFlow("dm-1" , DeviceManagerNew.dmw_1_ip_for_known, DeviceManagerNew.dmw_1_ip_for_known_dsc, "Known Devices", "Cross Reference Tables \\& improved Serialization"));
		//Timestamp Values
		simulations.put("dm-2-unknown", new WorkloadPerFlow("dm-2" , DeviceManagerNew.dmw_2_arp_for_unknown, DeviceManagerNew.dmw_2_arp_for_unknown_dsc, "ARP from Unknown Source", "Timestamp Values"));
		simulations.put("dm-2-known", new WorkloadPerFlow("dm-2" , DeviceManagerNew.dmw_2_ip_for_known, DeviceManagerNew.dmw_2_ip_for_known_dsc, "Known Devices", "Timestamp Values"));
		//Column Based 
		simulations.put("dm-3-unknown", new WorkloadPerFlow("dm-3" , DeviceManagerNew.dmw_3_arp_for_unknown, DeviceManagerNew.dmw_3_arp_for_unknown_dsc, "ARP from Unknown Source", "Columns"));
		simulations.put("dm-3-known", new WorkloadPerFlow("dm-3" , DeviceManagerNew.dmw_3_ip_for_known, DeviceManagerNew.dmw_3_ip_for_known_dsc, "Known Devices", "Columns"));
		//Micro Components
		simulations.put("dm-4-unknown", new WorkloadPerFlow("dm-4" , DeviceManagerNew.dmw_4_arp_for_unknown, DeviceManagerNew.dmw_4_arp_for_unknown_dsc, "ARP from Unknown Source", "Micro Componenets"));
		simulations.put("dm-4-known", new WorkloadPerFlow("dm-4" , DeviceManagerNew.dmw_4_ip_for_known, DeviceManagerNew.dmw_4_ip_for_known_dsc, "Known Devices", "Micro Components"));
	}
	
	static class DeviceManagerNew{
		public static String read_source_key = "Read the source device key"; 
		public static String read_source = "Read the source device"; 
		public static String update_ts = "Update \"last seen\" timestamp" ;
		public static String read_dest_key = "Read the destination device key"; 
		public static String read_dest = "Read the destination device"; 
		
		public static String inc_counter = "Get and increment the device id counter"; 
		public static String add_device_map = "Put new device in device table"; 
		public static String add_device_mac = "Put new device in \\texttt{(MAC,VLAN)} table";
		public static String get_ips = "Get devices with source IP"; 
		public static String update_ips = "Update devices with source IP";
		
		
		//KV basic 
		public static int[][] dmw_0_ip_for_known ={ 
				{ FlowSimulation.READ_OP,408,8},
				{ FlowSimulation.READ_OP,26,1444},
				{ FlowSimulation.WRITE_OP,2942,0},
				{ FlowSimulation.READ_OP,408,8},
				{ FlowSimulation.READ_OP,26,1369}
		}; 
		
		public static String[] dmw_0_ip_for_known_dsc ={
			read_source_key, 
			read_source,
			update_ts,
			read_dest_key, 
			read_dest
		};
		
		public static int[][] dmw_0_arp_for_unknown ={ 
			{ FlowSimulation.READ_OP,408,0},
			{ FlowSimulation.WRITE_OP,21,4},
			{ FlowSimulation.WRITE_OP,1395,1},
			{ FlowSimulation.WRITE_OP,416,0},
			{ FlowSimulation.READ_OP,386,0},
			{ FlowSimulation.WRITE_OP,517,0},
			{ FlowSimulation.READ_OP,408,8},
			{ FlowSimulation.READ_OP,26,1378},
		}; 
		public static String[] dmw_0_arp_for_unknown_dsc ={
					read_source_key, 
					inc_counter, 
					add_device_map, 
					add_device_mac, 
					get_ips, 
					update_ips, 
					read_dest_key, 
					read_dest,
		};
		
		// KV -  With Cross Reference and removed the Device Entity Class from the serialization (state ) of the objects 
		public static int[][] dmw_1_ip_for_known ={ 
			{ FlowSimulation.READ_OP,408,1274},
			{ FlowSimulation.WRITE_OP,2602,0},
			{ FlowSimulation.READ_OP,408,1199}
		};
		
		public static String[] dmw_1_ip_for_known_dsc ={
			read_source,
			update_ts,
			read_dest
		};
		
	public static int[][] dmw_1_arp_for_unknown ={ 
		{ FlowSimulation.READ_OP,408,0},
		{ FlowSimulation.WRITE_OP,21,4},
		{ FlowSimulation.WRITE_OP,1225,1},
		{ FlowSimulation.WRITE_OP,416,0},
		{ FlowSimulation.READ_OP,386,0},
		{ FlowSimulation.WRITE_OP,517,0},
		{ FlowSimulation.READ_OP,408,1208},
	};
	
	public static String[] dmw_1_arp_for_unknown_dsc ={
				read_source, 
				inc_counter, 
				add_device_map, 
				add_device_mac, 
				get_ips, 
				update_ips, 
				read_dest, 
	};
	
	// KV -   Timestamped based 
	
	public static int[][] dmw_2_ip_for_known ={ 
		{ FlowSimulation.READ_OP,408,1278},
		{ FlowSimulation.WRITE_OP,1316,1},
		{ FlowSimulation.READ_OP,408,1203},
	};
	
	public static String[] dmw_2_ip_for_known_dsc ={
		read_source,
		update_ts,
		read_dest
	};
	public static int[][] dmw_2_arp_for_unknown ={ 
		{ FlowSimulation.READ_OP,408,0},
		{ FlowSimulation.WRITE_OP,21,4},
		{ FlowSimulation.WRITE_OP,1225,1},
		{ FlowSimulation.WRITE_OP,416,0},
		{ FlowSimulation.READ_OP,386,0},
		{ FlowSimulation.WRITE_OP,517,0},
		{ FlowSimulation.READ_OP,408,1212},
	}; 
	public static String[] dmw_2_arp_for_unknown_dsc ={
				read_source, 
				inc_counter, 
				add_device_map, 
				add_device_mac, 
				get_ips, 
				update_ips, 
				read_dest, 
	};
	// Column Based
	public static int[][] dmw_3_ip_for_known ={ 
		{ FlowSimulation.READ_OP,486,1261},
		{ FlowSimulation.WRITE_OP,667,1},
		{ FlowSimulation.READ_OP,416,474},
	};
	
	public static String[] dmw_3_ip_for_known_dsc ={
		read_source, 
		update_ts,
		read_dest + "(partially)"
	};
	
	public static int[][] dmw_3_arp_for_unknown ={ 
		{ FlowSimulation.READ_OP,486,0},
		{ FlowSimulation.WRITE_OP,21,4},
		{ FlowSimulation.WRITE_OP,1183,1},
		{ FlowSimulation.WRITE_OP,416,0},
		{ FlowSimulation.READ_OP,386,0},
		{ FlowSimulation.WRITE_OP,517,0},
		{ FlowSimulation.READ_OP,416,474},
	}; 
	
	public static String[] dmw_3_arp_for_unknown_dsc ={
				read_source, 
				inc_counter, 
				add_device_map, 
				add_device_mac, 
				get_ips, 
				update_ips, 
				read_dest + "(partially)", 
	};
	
	// Micro Componenets 
	
	public static int[][] dmw_4_ip_for_known ={
		{ FlowSimulation.READ_OP,28,1414},
		{ FlowSimulation.WRITE_OP,36,0},
	};
	
	public static String[] dmw_4_ip_for_known_dsc ={
		"Read source and destination devices (partially)", 
		"Update devices",
	};
	
	
	public static int[][] dmw_4_arp_for_unknown ={ 
		{ FlowSimulation.READ_OP,28,201},
		{ FlowSimulation.WRITE_OP,476,8},
	}; 
	
	public static String[] dmw_4_arp_for_unknown_dsc = {
		"Read source and destination devices (partially)",
		"Create source device in the data store", 
	};
	
	/*
	public static int[][] dmw_4_arp_for_unknown ={ 
		{ FlowSimulation.READ_OP,486,0},
		{ FlowSimulation.WRITE_OP,21,4},
		{ FlowSimulation.WRITE_OP,1183,1},
		{ FlowSimulation.WRITE_OP,416,0},
		{ FlowSimulation.READ_OP,386,0},
		{ FlowSimulation.WRITE_OP,517,0},
		{ FlowSimulation.READ_OP,416,474},
	}; 
	
	public static String[] dmw_4_arp_for_unknown_dsc ={
				read_source, 
				inc_counter, 
				add_device_map, 
				add_device_mac, 
				get_ips, 
				update_ips, 
				read_dest + "(partially)", 
	};
	*/
	}
	/*static class DeviceManager{
		
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
	}*/
	//FlowSimulation newPing = new DeviceManager(DeviceManager.NewPingingOneExistentIn1000H); 
	//FlowSimulation existentPing = new DeviceManager(DeviceManager.PingKnown1000H);
	int index=0;
	

	/*static{
		//Learning Switch 
		simulations.put("lsw-0", new WorkloadPerFlow("lsw-0" , LearningSwitch.lsw_0_broadcast_ops, LearningSwitch.lsw_0_brodcast_dsc, "Broadcast Packet"));  
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

		
		simulations.put("lsw-2", new OpenFlowMessageWorkload(new FlowSimulation[] {
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
		
	}*/

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
			s.append(en.getKey() + "#" + en.getValue().name  + "#" + en.getValue().workload_dsc +"#" +  en.getValue().datastoreState +"#\n");
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
		if (i > 5000){
			latency.addValue(total);
		}
	}
	
	public static void main(String[] args){
		PrintWriter writer;
		
		try {
			writer = new PrintWriter("//Users/fabiim/Dropbox/TESE/data/reportGenerator/descriptions", "UTF-8");
		
			for (Entry<String, WorkloadPerFlow> s : simulations.entrySet()){
				writer.print(s.getKey() + ": ");
				for (String m : s.getValue().ops_dsc ){
					writer.print(m +':') ;
				}
				writer.println("");
			}
			writer.flush(); 
			writer.close(); 
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
}
