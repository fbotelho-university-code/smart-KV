/**
 * 
 */
package smartkv.datastore.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * @author fabiim
 *
 */

//FIXME: At the moment we have to have a different JavaSerializer for each Collection implementation....
//TODO -Try to make performance better. 

public class JavaSerializer<T extends Serializable>  implements Serializer<T>{
	private JavaSerializer(){}
	
	public static <T extends Serializable>  JavaSerializer<T> getJavaSerializer(){
		return new JavaSerializer<T>(); 
	}
	
	
	
	/* 
	 * @see bonafide.datastore.util.Serializer#serialize(java.lang.Object)
	 */
	@Override
	public byte[] serialize(T obj) {
		ByteArrayOutputStream bout;
		ObjectOutputStream oout; 
		
		try {
			bout = new ByteArrayOutputStream(); 
			oout = new ObjectOutputStream(bout);
			oout.writeObject(obj);
			//This makes a copy of the internal array. 
			return bout.toByteArray(); 
		} catch (IOException e) {
			; 
		}
		return null; 
	}
	
	/* 
	 * @see bonafide.datastore.util.Serializer#deserialize(byte[])
	 */
	@Override
	public T deserialize(byte[] bytes) {
		ByteArrayInputStream in = new ByteArrayInputStream(bytes);
		ObjectInputStream oin;
		try {
			oin = new ObjectInputStream(in);
			return (T) oin.readObject();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		return null; 
	} 

	
}
