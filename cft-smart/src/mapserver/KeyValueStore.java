/**
 * 
 */
package mapserver;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import mapserver.util.LRULinkedHashMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;

/**
 * @author fabiim
 *
 */

class VersionedDataBucket{
	private byte[] val=null; 
	private int version =0;
	
	public VersionedDataBucket (byte[] val){
		this.val = val; 
	}
	public VersionedDataBucket(VersionedDataBucket v){
		this.val = v.getVal(); 
		this.version = v.getVersion(); 
	}
	public byte[] getVal() {
		return val;
	}
	public void setVal(byte[] val) {
		
		if (!Arrays.equals(val, this.val)){
			incrementVersion();
		}
		this.val = val;
	}
	
	private void incrementVersion() {
		version = (version +1) % (Integer.MAX_VALUE-1);  
	}
	
	public int getVersion() {
		return version;
	}
}


class VersionMap{
	Map<ByteArrayWrapper, VersionedDataBucket> values;

	public void clear() {
		values.clear();
	}

	public boolean containsKey(Object key) {
		return values.containsKey(key);
	}

	public boolean containsValue(Object value) {
		return values.containsValue(value);
	}

	public Set<Entry<ByteArrayWrapper, VersionedDataBucket>> entrySet() {
		return values.entrySet();
	}
	
	@Override
	public boolean equals(Object o) {
		return values.equals(o);
	}

	public VersionedDataBucket get(Object key) {
		return values.get(key);
	}

	public int hashCode() {
		return values.hashCode();
	}
	
	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return values.toString();
	}

	public boolean isEmpty() {
		return values.isEmpty();
	}

	public Set<ByteArrayWrapper> keySet() {
		return values.keySet();
	}
	
	public VersionedDataBucket put(ByteArrayWrapper key, byte[] data) {
		VersionedDataBucket tsBucket = values.get(key);
		if (tsBucket != null){
			VersionedDataBucket oldBucket = new VersionedDataBucket(tsBucket); 
			tsBucket.setVal(data);
			return oldBucket; 
		}
		values.put(key, new VersionedDataBucket(data)); 
		return null; 
	}
	
	public void putWithNoReturn(ByteArrayWrapper key, byte[] data){
		VersionedDataBucket tsBucket = values.get(key);
		if (tsBucket != null){
			tsBucket.setVal(data);
			return; 
		}
		values.put(key, new VersionedDataBucket(data)); 
	}
	
	public VersionedDataBucket remove(Object key) {
		return values.remove(key);
	}
	
	public int size() {
		return values.size();
	}
	
	public Collection<VersionedDataBucket> values() {
		return values.values();
	}
}

public class KeyValueStore implements Datastore, Serializable{
	public static final byte[] TRUE = new byte[0]; 
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Map<String, Map<ByteArrayWrapper, byte[]>> datastore;
	private Map<String, String> pointers; 
	
	public KeyValueStore(){
		 datastore = new HashMap<String, Map<ByteArrayWrapper,byte[]>>();
		 pointers = Maps.newHashMap(); 
	}
	
	@Override
	public byte[] get_and_increment(DataInputStream dis) throws IOException, ClassNotFoundException{
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 //FIXME
			 byte[] key = dis.readUTF().getBytes(); 
			 byte[] val = datastore.get(tableName).get(new ByteArrayWrapper(key));
			 if (val != null){
				 //FIXME: move me to another key value store... With ints...
				 int l =(Integer) Ints.fromByteArray(val);  
				 datastore.get(tableName).put(new ByteArrayWrapper(key), Ints.toByteArray(l +1));
				 return Ints.toByteArray(l); 
			 }
			 else{
				 datastore.get(tableName).put(new ByteArrayWrapper(key),  Ints.toByteArray(1));
				 return Ints.toByteArray(0); 
			 }
		 }
		 return null; 
	}
		
	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] create_table(DataInputStream dis) throws IOException {
		String tableName = dis.readUTF();
		 if (!datastore.containsKey(tableName)){
			 datastore.put(tableName, new HashMap<ByteArrayWrapper,byte[]>());
			 return TRUE;  
		 }
		 return null;
	}

	@Override 
	public byte[] create_pointer_table(DataInputStream dis) throws IOException{
		String tableName = dis.readUTF();
		String destinyTable = dis.readUTF();
//		System.out.println("K Creating pointers from :" + tableName + " to : " + destinyTable);

		if (!datastore.containsKey(tableName) && datastore.containsKey(destinyTable)){
			//System.out.println("created"); 
			 pointers.put(tableName, destinyTable);
			 datastore.put(tableName, new HashMap<ByteArrayWrapper,byte[]>()); 
			 return TRUE;  
		 }
		 return null;
			//FIXME deleted tables and such.
	}
	
	@Override
	public byte[] get_referenced_value(DataInputStream dis) throws IOException{
		String tableName = dis.readUTF();
		//System.out.println("K Get references:" + tableName);
		if (pointers.containsKey(tableName)){
			//System.out.println("K In pointers" );
			ByteArrayWrapper key = new ByteArrayWrapper(readNextByteArray(dis));
			Map<ByteArrayWrapper, byte[]> keysTable = datastore.get(tableName);
			if (keysTable.containsKey(key)){
				//System.out.println("K I have the key" );
				Map<ByteArrayWrapper, byte[]> endTable = datastore.get(pointers.get(tableName));
				//System.out.println(endTable.containsKey(new ByteArrayWrapper(keysTable.get(key))));
				byte[] b = endTable.get(new ByteArrayWrapper(keysTable.get(key)));
				//System.out.println(b); 
				return b; 
			}
		}
		return null; 
	}
	
	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] create_table_max_size(DataInputStream dis)
			throws IOException {
		String tableName;
		tableName = dis.readUTF(); 
		if (!datastore.containsKey(tableName)){
			 int size = dis.read(); 
			 datastore.put(tableName, new LRULinkedHashMap<ByteArrayWrapper,byte[]>(size)); 
			 return TRUE;  
		 }
		 return null;
	}

	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] remove_table(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 datastore.remove(tableName); //TODO - remove hack
			 if (pointers.containsKey(tableName)){
				 pointers.remove(tableName);
			 }
			 //FIXME we should not traverse every pointer table and delete references to deleted tables. 
			 return TRUE; 
		 }
		 return null;
	}

	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] contains_table(DataInputStream dis) throws IOException {
		String needle = dis.readUTF(); 
		 if (datastore.containsKey(needle)){
			 return TRUE;  
		 }
		/* else if (pointers.containsKey(needle)){
			 return TRUE; 
		 }*/
		 return null;
	}

	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] clear_table(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF();
		 if (datastore.containsKey(tableName)){
			 Map<?,?> m = datastore.get(tableName); 
			 if (m != null){
				 m.clear(); 
				 return TRUE;  
			 }
		 }
		/* else if (pointers.containsKey(tableName)){
			 pointers.get(tableName).key2key.clear();
			 return TRUE; 
		 }*/
		 return null;
	}

	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] contains_key_in_table(
			DataInputStream dis) throws IOException {

		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 Map<?,?> tableHayStack = datastore.get(tableName);
			 byte[] key = readNextByteArray(dis); 
			 if (tableHayStack.containsKey(new ByteArrayWrapper(key))){
				 return TRUE;  
			 }
		 }
		/* else if (pointers.containsKey(tableName)){
			 Map<?,?> tableHayStack = pointers.get(tableName).key2key;
			 byte[] key = readNextByteArray(dis);
			 if (tableHayStack.containsKey(new ByteArrayWrapper(key))){
				 return TRUE;  
			 }
		 }
*/
		 return null;
	}

	
	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] get_table(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF();
		 if (datastore.containsKey(tableName)){ 
			 Map<ByteArrayWrapper,byte[]> allTable = datastore.get(tableName);
			 Map<byte[],byte[]> c = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
			 for (Entry<ByteArrayWrapper, byte[]> en: allTable.entrySet()){
				 c.put(en.getKey().value, en.getValue());

			 }
			  byte[] ret  = MapSmart.serialize(c); //TODO: allTable is always != than null if table contains the key. We are the only ones to use it.

			 return ret; 
		 }
		/* else if (pointers.containsKey(tableName)){
			 throw new UnsupportedOperationException("Not yet implemented!"); 
		 }*/
		 return null;
	}

	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] get_value_in_table(
			DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 byte[] key =readNextByteArray(dis);
			 return datastore.get(tableName).get(new ByteArrayWrapper(key));
		 }
		/* else if (pointers.containsKey(tableName)){
			 ByteArrayWrapper  key =new ByteArrayWrapper(readNextByteArray(dis));
			 Pointers2Table pointer  = pointers.get(tableName);
			 if (pointer.key2key.containsKey(key)){
				 //Will return the pointed value from the intermediate map. 
				 Map<ByteArrayWrapper,byte[]> endTable = datastore.get(pointer.table); 
				 return endTable.get(pointer.key2key.get(key)); 
			 }

		 }*/
		 return null;
	}

	/**
	 * @return
	 */
	@Override
	public byte[] is_datastore_empty() {
		if (datastore.isEmpty()){
			 return TRUE;  
		 }
		 return null;
	}

	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] is_table_empty(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName) ){
			 Map<?,?> table = datastore.get(tableName); 
			 if (table.isEmpty()){
				 return TRUE;  
			 }
		 }
		/* else if (pointers.containsKey(tableName)){
			 if (pointers.get(tableName).key2key.isEmpty()){
				 return TRUE; 
			 }
		 }*/
		 return null;
	}

	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] put_value_in_table(
			DataInputStream dis) throws IOException {
		
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
		
			 final byte[] key = readNextByteArray(dis);
		
			 
			 final byte[] value =  ByteStreams.toByteArray(dis);
		
			 Map<ByteArrayWrapper, byte[]> table = datastore.get(tableName);
			 return table.put(new ByteArrayWrapper(key), value); 
			 
		 }
		/* else if(pointers.containsKey(tableName)){
			 final byte[] key = readNextByteArray(dis);
			 final byte[] value =  ByteStreams.toByteArray(dis);
			 ByteArrayWrapper val = pointers.get(tableName).key2key.put(new ByteArrayWrapper(key), new ByteArrayWrapper(value));
			 return val != null  ? val.value : null;  
		 }*/
		 return null;
	}

	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] put_Values_in_table(DataInputStream dis) throws IOException {
		String tableName;
		
		tableName =dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 Map<ByteArrayWrapper,byte[]> table = datastore.get(tableName);
			 byte[] valuesInBytes = ByteStreams.toByteArray(dis);
			 if (valuesInBytes != null){
				 try {
					 @SuppressWarnings("unchecked")
					 Map<byte[],byte[]> values = (Map<byte[], byte[]>) MapSmart.deserialize(valuesInBytes);
					 Map<ByteArrayWrapper, byte[]> realValues = Maps.newHashMap();
					 for (Entry<byte[],byte[]> en : values.entrySet()){
						 realValues.put(new ByteArrayWrapper(en.getKey()), en.getValue());
					 }
					 table.putAll(realValues);
					 return TRUE;  
				 } catch (ClassNotFoundException e) {
					 ; 
				 } 
			 }
		 }
		/* else if (pointers.containsKey(tableName)){
			 throw new UnsupportedOperationException("Not yet Implemented!"); 
		 }*/
		 return null;
	}

	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] remove_value_from_table(
			DataInputStream dis) throws IOException {
			String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 Map<ByteArrayWrapper,byte[]> table = datastore.get(tableName);
			 ByteArrayWrapper key = new ByteArrayWrapper(readNextByteArray(dis));
			 
			 if (table.containsKey(key)){
			
				 return table.remove(key);
			 }
		 }
		/* else if (pointers.containsKey(tableName)){
			 ByteArrayWrapper key = new ByteArrayWrapper(readNextByteArray(dis));
			 ByteArrayWrapper val =  pointers.get(tableName).key2key.remove(key);
			 return val != null? val.value : null; 
		 }*/
		 return null;
	}

	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] size_of_table(DataInputStream dis) throws IOException {
		String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 return Ints.toByteArray((datastore.get(tableName).size())); 
		 }
		/* else if (pointers.containsKey(tableName)){
			 return Ints.toByteArray((pointers.get(tableName).key2key.size())); 
		 }*/
		 return null;
	}
	
	
	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] atomic_replace_value_in_table(
			DataInputStream dis) throws IOException {
		String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 //TODO - efficient/safe - maybe use
			 ByteArrayWrapper k = new ByteArrayWrapper(readNextByteArray(dis));
			 Map<ByteArrayWrapper,byte[]> table = datastore.get(tableName);
			 if (table.containsKey(k)){
				byte[] oldValue = readNextByteArray(dis);
				 if (Arrays.equals(table.get(k), oldValue)){

					 final byte[] newValue = ByteStreams.toByteArray(dis);
					 table.put(k, newValue);
					 
					 return TRUE;  
				 }
			 }
		 }
		/* else if (pointers.containsKey(tableName)){
			 ByteArrayWrapper key = new ByteArrayWrapper(readNextByteArray(dis));			 
			 if (pointers.get(tableName).key2key.containsKey(key)){
				 Pointers2Table pointer = pointers.get(tableName);
				 ByteArrayWrapper oldValue = new ByteArrayWrapper(readNextByteArray(dis));
				 if (pointer.key2key.get(key).equals(oldValue)){
					 ByteArrayWrapper newValue = new ByteArrayWrapper(readNextByteArray(dis));
					 pointers.put(key, value);
				 }
				 return TRUE;
			 }
		 }*/
		 return null;
	}

	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] atomic_remove_if_value(
			DataInputStream dis) throws IOException {
		String tableName = dis.readUTF();
		 ByteArrayWrapper  key = new ByteArrayWrapper(readNextByteArray(dis));
		 if (datastore.containsKey(tableName)){
			 Map<ByteArrayWrapper,byte[]> table = datastore.get(tableName);
			 if (table.containsKey(key)){
				 byte[] currValue = ByteStreams.toByteArray(dis);
				 if (Arrays.equals(table.get(key), currValue)){
					 table.remove(key); 
					 return TRUE; 
				 }
			 }
		 }
		 return null;
	}

	/**
	 * @param in
	 * @param dis
	 * @return 
	 * @throws IOException
	 */
	@Override
	public byte[] atomic_put_if_absent(
			DataInputStream dis) throws IOException {
		String tableName;
		ByteArrayWrapper key;
		tableName = dis.readUTF(); 
		key= new ByteArrayWrapper(readNextByteArray(dis));
		if (datastore.containsKey(tableName)){
			Map<ByteArrayWrapper,byte[]> table = datastore.get(tableName);
			if (!table.containsKey(key)){
				return table.put(key, ByteStreams.toByteArray(dis));
			}
			else{
				return table.get(key);
			}
		}
		return null;
	}
	
	private byte[] readNextByteArray(DataInputStream in) throws IOException {
		int size =  in.readInt(); 
		byte[] result = new byte[size];
		in.readFully(result);
		return result; 
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#clear()
	 */
	@Override
	public byte[] clear() throws Exception {
		datastore.clear();
		return null; 
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#insert_value_in_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] insert_value_in_table(DataInputStream dis) throws Exception{
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 final byte[] key = readNextByteArray(dis); 
			 final byte[] value =  ByteStreams.toByteArray(dis);
			 Map<ByteArrayWrapper, byte[]> table = datastore.get(tableName);
			 table.put(new ByteArrayWrapper(key), value); 
			 return TRUE;  
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#values(java.io.DataInputStream)
	 */
	@Override
	public byte[] values(DataInputStream msg) throws Exception {
			String tableName = msg.readUTF(); 
			if (datastore.containsKey(tableName)){
				//FIXME : get smart to check this return value with unordered maps. 
				Map<ByteArrayWrapper, byte[]> table = datastore.get(tableName);
				Collection<byte[]> values = table.values();
				List<byte[]> finalValues= Lists.newArrayList(values);
				Collections.sort(finalValues, UnsignedBytes.lexicographicalComparator());

				return MapSmart.serialize(finalValues); 
			}
			return null; 
		}
}
