/**
 * 
 */
package datastore.bench;

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
	}

}
