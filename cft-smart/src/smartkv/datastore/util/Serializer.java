/**
 * 
 */
package smartkv.datastore.util;

import com.google.common.primitives.Longs;

/**
 * @author fabiim
 *
 */
public interface Serializer<T> {
	public static  Serializer<Long> LONG = new Serializer<Long>(){

		@Override
		public byte[] serialize(Long obj) {
			return Longs.toByteArray(obj);
		}

		@Override
		public Long deserialize(byte[] bytes) {
			return Longs.fromByteArray(bytes); 
		}
		
	};
	public byte[] serialize(T obj);
	public T deserialize(byte[] bytes); 
}
