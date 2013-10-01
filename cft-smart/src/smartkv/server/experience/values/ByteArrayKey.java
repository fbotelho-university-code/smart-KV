/**
 * 
 */
package smartkv.server.experience.values;

import java.io.Serializable;
import java.util.Arrays;

import smartkv.server.ByteArrayWrapper;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedBytes;

/**
 * @author fabiim
 *
 */
public class ByteArrayKey implements Key {
	private static final long serialVersionUID = 1L;

	public static ByteArrayKey createKeyFromBytes(byte[] b){
		return new ByteArrayKey(b); 
	}
		
	//TODO - change this for something that can be serializable?
	public static final HashFunction hf = Hashing.murmur3_32();
		

	public final byte[] value;
	
	/* (non-Javadoc)
	 * @see smartkv.server.experience.values.Key#asByteArray()
	 */
	@Override
	public byte[] asByteArray() {
		return value; 
	}
	
	public ByteArrayKey(byte[] v){
		value = v; 
	}


	
	/* (non-Javadoc)
	 * @see smartkv.server.experience.values.Key#equals(java.lang.Object)
	 */

	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ByteArrayKey other = (ByteArrayKey) obj;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}
	
	//FIXME - not sure if usigned/signed would cause problems. All I care is that they deterministic across replicas, so i guess it's safe. 
	@Override
	public int compareTo(Key arg0) {
		return UnsignedBytes.lexicographicalComparator().compare(asByteArray(), arg0.asByteArray()); 
	}

	@Override
	public  final
	int hashCode(){
		return hf.hashBytes(value).asInt(); 
		
	}
	
	@Override
	public String toString() {
		return "Key ["+   Arrays.toString(value) + "]";
	}

	

}
