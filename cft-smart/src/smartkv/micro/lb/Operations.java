/**
 * 
 */
package smartkv.micro.lb;


import net.floodlightcontroller.loadbalancer.LBMember;
import net.floodlightcontroller.loadbalancer.LBPool;
import smartkv.client.KeyValueProxy;
import smartkv.client.tables.IKeyValueTable;
import smartkv.client.tables.KeyValueTable_;
import smartkv.client.tables.TableBuilder;
import smartkv.client.tables.VersionedValue;
import smartkv.client.util.Serializer;
import smartkv.server.MapSmart;
import smartkv.server.RequestType;

/**
 * @author fabiim
 *
 */
public class Operations {
	public  IKeyValueTable<String, LBPool> pools;
	public  IKeyValueTable<String, LBMember> members;
	
	
	public Operations(final MapSmart m){
		TableBuilder<String,LBPool> builder = new TableBuilder<String,LBPool>().setProxy(new KeyValueProxy(0){
			@Override
			protected byte[] invokeRequestWithRawReturn(RequestType type,
					byte[] request) {
				return m.execute(request);
			}
		})
		.setTableName("LB-POOLS"); 

		TableBuilder<String,LBMember> membersBuilder = new TableBuilder<String,LBMember>().setProxy(new KeyValueProxy(0){
			@Override
			protected byte[] invokeRequestWithRawReturn(RequestType type,
					byte[] request) {
				return m.execute(request);
			}
		})
		.setTableName("LB-MEMBERS"); 
		
		pools = new KeyValueTable_<String,LBPool>(builder); 
		members = new KeyValueTable_<String,LBMember>(membersBuilder); 
	}
	
	public  byte[] roundRobin(String id){
        	VersionedValue<LBPool> pool = pools.getWithTimeStamp(id); // FIXME can be null. 
        	String memberId = pool.value().pickMember();
        	pools.replace(id,pool.version(), pool.value());
        	Integer address = members.get(memberId).getAddress();
        	return Serializer.INT.serialize(address);
		return null; 
	}
}
