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
public class KeyValueStore implements Datastore, Serializable{
	
	
	
	
	
	private Map<String, Map<ByteArrayWrapper, byte[]>> datastore;
	
	public KeyValueStore(){
		 datastore = new HashMap<String, Map<ByteArrayWrapper,byte[]>>();
	}
	
	@Override
	public byte[] get_and_increment(DataInputStream dis) throws IOException, ClassNotFoundException{
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 //FIXME
			 byte[] key =dis.readUTF().getBytes(); 
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
			 return new byte[1]; //TODO - remove hack. 
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
			 return new byte[1]; 
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
			 return new byte[1];
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
			 return new byte[1]; 
		 }
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
				 return new byte[1]; 
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
	public byte[] contains_key_in_table(
			DataInputStream dis) throws IOException {

		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 Map<?,?> tableHayStack = datastore.get(tableName);
			 byte[] key = readNextByteArray(dis); 
			 if (tableHayStack.containsKey(new ByteArrayWrapper(key))){
				 return new byte[1]; 
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
	public byte[] get_table(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF();
		boolean d= false;
		
		
		 if (datastore.containsKey(tableName)){ 
			 Map<ByteArrayWrapper,byte[]> allTable = datastore.get(tableName);
			 

			 Map<byte[],byte[]> c = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
			 for (Entry<ByteArrayWrapper, byte[]> en: allTable.entrySet()){
				 c.put(en.getKey().value, en.getValue());

			 }
			  byte[] ret  = MapSmart.serialize(c); //TODO: allTable is always != than null if table contains the key. We are the only ones to use it.

			 return ret; 
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
	public byte[] get_value_in_table(
			DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 byte[] key =readNextByteArray(dis);
			 return datastore.get(tableName).get(new ByteArrayWrapper(key));
		 }
		 return null;
	}

	/**
	 * @return
	 */
	@Override
	public byte[] is_datastore_empty() {
		if (datastore.isEmpty()){
			 return new byte[1]; 
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
		 if (datastore.containsKey(tableName)){
			 Map<?,?> table = datastore.get(tableName); 
			 if (table.isEmpty()){
				 return new byte[1]; 
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
					 return new byte[1]; 
				 } catch (ClassNotFoundException e) {
					 ; 
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
					 
					 return new byte[1]; 
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
					 return new byte[1];
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
			 return new byte[1]; 
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
