/**
 * 
 */
package smartkv.client;

import com.google.common.primitives.Ints;

/**
 * @author fabiim
 *
 */
public class VersionedDatastoreValue extends DatastoreValue{
	public  final int ts;
	
	public VersionedDatastoreValue(byte[] reply) {
		super(reply, 4, reply.length-1);
		this.ts = Ints.fromByteArray(reply);
	}
	
	public int getVersion() {
		return ts;
	}
	
}