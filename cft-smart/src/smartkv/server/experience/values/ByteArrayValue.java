/**
 * 
 */
package smartkv.server.experience.values;

import java.util.Arrays;

/**
 * @author fabiim
 *
 */
public class ByteArrayValue implements Value{

	private byte[] array; 
	/**
	 * @param array
	 */
	public ByteArrayValue(byte[] array) {
		this.array = array; 
	}

	public static Value createValueFromByteArray(byte[] array){
		return new ByteArrayValue(array); 
	}
	
	/* (non-Javadoc)
	 * @see smartkv.server.experience.values.Value#asByteArray()
	 */
	@Override
	public byte[] asByteArray() {
		return array; 
	}

	@Override
	public String toString() {
		return "ByteArrayValue [array=" + Arrays.toString(array) + "]";
	}

	/* (non-Javadoc)
	 * @see smartkv.server.experience.values.Value#arrangeDataDeterministically()
	 */
	@Override
	public void arrangeDataDeterministically() {
		// TODO Auto-generated method stub
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(array);
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ByteArrayValue other = (ByteArrayValue) obj;
		if (!Arrays.equals(array, other.array))
			return false;
		return true;
	}

}
