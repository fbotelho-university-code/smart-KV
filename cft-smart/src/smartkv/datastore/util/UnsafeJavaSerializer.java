/**
 * 
 */
package smartkv.datastore.util;

import java.io.Serializable;

/**
 * @author fabiim
 *
 */
public class UnsafeJavaSerializer<T>  implements Serializer<T>{

	
	public static <T>  UnsafeJavaSerializer<T> getInstance(){
		return new UnsafeJavaSerializer<T>(); 
	}
	
	
	private UnsafeJavaSerializer(){;}
	JavaSerializer<Serializable> serializer =JavaSerializer.getJavaSerializer();  
	/* (non-Javadoc)
	 * @see bonafide.datastore.util.Serializer#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(T obj) {
		return serializer.serialize((Serializable) obj); 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.util.Serializer#deserialize(byte[])
	 */
	@SuppressWarnings("unchecked")
	@Override
	public T deserialize(byte[] bytes) {
		return (T) serializer.deserialize(bytes); 
	}

	
}
