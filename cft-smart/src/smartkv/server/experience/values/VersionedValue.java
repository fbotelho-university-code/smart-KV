/**
 * 
 */
package smartkv.server.experience.values;

import java.util.Arrays;

import com.google.common.primitives.Ints;


/**
 * @author fabiim
 *
 */
public class VersionedValue implements Value{
		
	private static final long serialVersionUID = 1L;
	Value value;
	int timestamp; 
	
	public VersionedValue(Value v){
		
		timestamp = 0; 
		value = v; 
	}
	
	
	public VersionedValue(Value value, int timestamp) {
		super();
		this.value = value;
		this.timestamp = timestamp;
	}


	public Value getValue() {
		return value; 
	}

	public void setValue(Value value2) {
		if (!this.value.equals(value2)){
			value = value2; 
			timestamp = (timestamp +1) % (Integer.MAX_VALUE-1);  
		}
	}


	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + timestamp;
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}


	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (obj instanceof Value){
			if (obj instanceof VersionedValue){
				VersionedValue other = (VersionedValue) obj;
				return this.value.equals(other.getValue());
			}
			else {
				return this.value.equals(obj); 
			}
		}
		return false;
	}


	@Override
	public byte[] asByteArray() {
		byte[] valueBytes = value.asByteArray();
		int valueBytesLen = valueBytes.length;
		byte[] valueAndTimestampBytes = new byte[valueBytesLen + 4 ]; 
		System.arraycopy(valueBytes, 0, valueAndTimestampBytes, 4, valueBytesLen);
		byte[] tsByteArray = serializeTimestamp(); 
		System.arraycopy(tsByteArray, 0, valueAndTimestampBytes, 0, tsByteArray.length);
	
		return valueAndTimestampBytes;
	}

	@Override
	public void arrangeDataDeterministically() {
		// TODO Auto-generated method stub
	}
	
	private byte[] serializeTimestamp(){
		return Ints.toByteArray(timestamp); 
	}

	@Override
	public String toString() {
		return "VersionedValue [value=" + value + ", timestamp=" + timestamp
				+ "]";
	}


	/**
	 * @return
	 */
	public int getVersion() {
		return timestamp;
	}

	
}
