/**
 * 
 */
package smartkv.server;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import smartkv.server.util.LRULinkedHashMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;


//TODO: logger prints only in one replica. 

/**
 * @author fabiim
 *
 */

//FIXME : this screams memory leak all over the place. The key, map cannot be garbage collected after it has been eliminated. 


public class KeyValueColumnStore implements ColumnDatastore, Serializable{
	
	private Map<String, Map<ByteArrayWrapper, Map<String,byte[]>>> datastore;
	
	private Map<String, Map<String,Integer>> counters;
	
	//FIXME :Addd timestamps map.
	public KeyValueColumnStore(){
		 datastore = new HashMap<String, Map<ByteArrayWrapper, Map<String,byte[]>>>();
		 counters = new HashMap<String,Map<String,Integer>>(); 
		 pointers = Maps.newHashMap(); 
	}
	
	/* (non-Javadoc)
	 * @see mapserver.Datastore#get_and_increment(java.io.DataInputStream)
	 */
	@Override
	public byte[] get_and_increment(DataInputStream dis) throws IOException, ClassNotFoundException{
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 if (!counters.containsKey(tableName)){
				 counters.put(tableName, new HashMap<String,Integer>());
				 //FIXME : memory leak.. values are never deleted...
			 }
			 //FIXME
			 String key =dis.readUTF(); 
			 Integer val = counters.get(tableName).get(key);
			 if (val != null){
				 //FIXME: move me to another key value store... With ints...
				 counters.get(tableName).put(key, val +1);
				 return Ints.toByteArray(val); 
			 }
			 else{
				 counters.get(tableName).put(key, 1);
				 return Ints.toByteArray(0); 
			 }
		 }
		 return null; 
	}
	/* (non-Javadoc)
	 * @see mapserver.Datastore#create_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] create_table(DataInputStream dis) throws Exception {
		String tableName = dis.readUTF();
		if (!datastore.containsKey(tableName)){
			 datastore.put(tableName, new HashMap<ByteArrayWrapper,Map<String,byte[]>>());
			 return new byte[1]; 
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#create_table_max_size(java.io.DataInputStream)
	 */
	@Override
	public byte[] create_table_max_size(DataInputStream dis) throws Exception {
		String tableName;
		tableName = dis.readUTF(); 
		 if (!datastore.containsKey(tableName)){
			 int size = dis.read(); 
			 datastore.put(tableName, new LRULinkedHashMap<ByteArrayWrapper,Map<String,byte[]>>(size)); 
			 return new byte[1]; 
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#remove_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] remove_table(DataInputStream dis) throws Exception {
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 datastore.remove(tableName); //TODO - remove hack 
			 return new byte[1];
		 }
		 return null;
	}
	
	
	/* (non-Javadoc)
	 * @see mapserver.Datastore#contains_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] contains_table(DataInputStream dis) throws Exception {
		String needle = dis.readUTF(); 
		 if (datastore.containsKey(needle)){
			 return new byte[1]; 
		 }
		 return null;
		
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
	 * @see mapserver.Datastore#clear_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] clear_table(DataInputStream dis) throws Exception {
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

	/* (non-Javadoc)
	 * @see mapserver.Datastore#contains_key_in_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] contains_key_in_table(DataInputStream dis) throws Exception {
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

	/* (non-Javadoc)
	 * @see mapserver.Datastore#get_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] get_table(DataInputStream dis) throws Exception {
		String tableName;
		tableName = dis.readUTF();
		boolean d= false;
		 if (datastore.containsKey(tableName)){ 
			 Map<ByteArrayWrapper,Map<String,byte[]>> allTable = datastore.get(tableName);
			 Map<byte[],Map<String,byte[]>> c = Maps.newTreeMap(UnsignedBytes.lexicographicalComparator());
			 for (Entry<ByteArrayWrapper, Map<String,byte[]>> en: allTable.entrySet()){
				 c.put(en.getKey().value, en.getValue());
			 }
			  byte[] ret  = MapSmart.serialize(c); //TODO: allTable is always != than null if table contains the key. We are the only ones to use it.

			 return ret; 
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#get_value_in_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] get_value_in_table(DataInputStream dis) throws Exception {
		String tableName;
		tableName = dis.readUTF();
		
		 if (datastore.containsKey(tableName)){
			 byte[] key =readNextByteArray(dis);
			 byte[] val=  MapSmart.serialize(datastore.get(tableName).get(new ByteArrayWrapper(key)));
			 return val; 

		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#is_datastore_empty()
	 */
	@Override
	public byte[] is_datastore_empty() throws Exception {
		return datastore.isEmpty() ? new byte[1] : null; 
	}
		
	/* (non-Javadoc)
	 * @see mapserver.Datastore#is_table_empty(java.io.DataInputStream)
	 */
	@Override
	public byte[] is_table_empty(DataInputStream dis) throws Exception {
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

	/* (non-Javadoc)
	 * @see mapserver.Datastore#put_value_in_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] put_value_in_table(DataInputStream dis) throws Exception {
		
		String tableName;
		tableName = dis.readUTF();
		 if (datastore.containsKey(tableName)){
			 final byte[] key = readNextByteArray(dis);
			 @SuppressWarnings("unchecked")
			 Map<String,byte[]> value = (Map<String, byte[]>) MapSmart.deserialize(ByteStreams.toByteArray(dis));
			 Map<ByteArrayWrapper, Map<String,byte[]>> table = datastore.get(tableName);
			 return MapSmart.serialize(table.put(new ByteArrayWrapper(key), value)); 
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#put_Values_in_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] put_Values_in_table(DataInputStream dis) throws Exception {
		String tableName;
		tableName =dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 Map<ByteArrayWrapper,Map<String,byte[]>> table = datastore.get(tableName);
			 byte[] valuesInBytes = ByteStreams.toByteArray(dis);
			 if (valuesInBytes != null){
				 try {
					 @SuppressWarnings("unchecked")
					 Map<byte[],Map<String,byte[]>> values = (Map<byte[], Map<String,byte[]>>) MapSmart.deserialize(valuesInBytes);
					 Map<ByteArrayWrapper,Map<String, byte[]>> realValues = Maps.newHashMap();
					 for (Entry<byte[],Map<String,byte[]>> en : values.entrySet()){
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

	/* (non-Javadoc)
	 * @see mapserver.Datastore#remove_value_from_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] remove_value_from_table(DataInputStream dis) throws Exception {
		String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 Map<ByteArrayWrapper,Map<String,byte[]>> table = datastore.get(tableName);
			 ByteArrayWrapper key = new ByteArrayWrapper(readNextByteArray(dis));
			 if (table.containsKey(key)){
				 return MapSmart.serialize(table.remove(key));
			 }
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#size_of_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] size_of_table(DataInputStream dis) throws Exception {
		String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 return Ints.toByteArray((datastore.get(tableName).size())); 
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#atomic_replace_value_in_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] atomic_replace_value_in_table(DataInputStream dis)
			throws Exception {
		String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 //TODO - efficient/safe
			 ByteArrayWrapper k = new ByteArrayWrapper(readNextByteArray(dis));
			 Map<ByteArrayWrapper,Map<String,byte[]>> table = datastore.get(tableName);
			 if (table.containsKey(k)){
				Map<String,byte[]> oldValue = (Map<String, byte[]>) MapSmart.deserialize(readNextByteArray(dis));
				//XXX- maybe too heavyweight foo. Check this.
				if (areByteArrayValueMapEqual(table.get(k), oldValue)){
					 final Map<String,byte[]> newValue =(Map<String, byte[]>) MapSmart.deserialize( ByteStreams.toByteArray(dis));
					 table.put(k, newValue);
					 return new byte[1]; 
				 }
			 }
		 }
		 return null;
	}
	
	/**
	 * @param map
	 * @param oldValue
	 * @return
	 */
	//TODO: move to util... 
	private boolean areByteArrayValueMapEqual(Map<String, byte[]> a,
			Map<String, byte[]> b) {
		if (a.size() == b.size() ){
			for (Entry<String, byte[]>  en : a.entrySet()){
				if (b.containsKey(en.getKey()) && Arrays.equals(en.getValue(), b.get(en.getKey()))){
					continue;
				}
				return false; 
			}
			return true; 
		}
		return false; 
	}
	
	/* (non-Javadoc)
	 * @see mapserver.Datastore#atomic_remove_if_value(java.io.DataInputStream)
	 */
	@Override
	public byte[] atomic_remove_if_value(DataInputStream dis) throws Exception {
		String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 //TODO - efficient/safe - maybe use
			 ByteArrayWrapper k = new ByteArrayWrapper(readNextByteArray(dis));
			 Map<ByteArrayWrapper, Map<String, byte[]>> table = datastore.get(tableName);
			 if (table.containsKey(k)){
				@SuppressWarnings("unchecked")
				Map<String, byte[]>  oldValue = (Map<String, byte[]>) MapSmart.deserialize(ByteStreams.toByteArray(dis));
				if (areByteArrayValueMapEqual(table.get(k), oldValue)){
					table.remove(k); 
					return new byte[1]; 
				 }
			 }
		 }
		 return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.Datastore#atomic_put_if_absent(java.io.DataInputStream)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public byte[] atomic_put_if_absent(DataInputStream dis) throws Exception {
		String tableName;
		ByteArrayWrapper key;
		tableName = dis.readUTF(); 
		key= new ByteArrayWrapper(readNextByteArray(dis));
		if (datastore.containsKey(tableName)){
			Map<ByteArrayWrapper,Map<String,byte[]>> table = datastore.get(tableName);
			if (!table.containsKey(key)){
				return MapSmart.serialize(table.put(key, (Map<String, byte[]>) MapSmart.deserialize(ByteStreams.toByteArray(dis))));
			}
			else{
				return MapSmart.serialize(table.get(key));
			}
		}
		return null;
	}

	/* (non-Javadoc)
	 * @see mapserver.ColumnDatastore#get_column(java.io.DataInputStream)
	 */

	@Override
	public byte[] get_column(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF();
		 if (datastore.containsKey(tableName)){
			 byte[] key =readNextByteArray(dis);
			 Map<String,byte[]> value = datastore.get(tableName).get(new ByteArrayWrapper(key));
			 String columnName = dis.readUTF();
			 return value.get(columnName); 
		 }
		 return null;
	}
	
	
	/* (non-Javadoc)
	 * @see mapserver.ColumnDatastore#put_column(java.io.DataInputStream)
	 */
	@Override
	public byte[] put_column(DataInputStream dis) throws Exception{
		String tableName =dis.readUTF();
		//System.out.println("Setting column@" + tableName);
		if (datastore.containsKey(tableName)){
			byte[] key = readNextByteArray(dis);
			Map<String, byte[]> value = datastore.get(tableName).get(new ByteArrayWrapper(key));
			//System.out.println("Setting column : " + Arrays.toString(key) + "V =" + value);
			if (value != null) {
				String column=  dis.readUTF(); 
				if (value.containsKey(column)){
					value.put(column, ByteStreams.toByteArray(dis));
					return new byte[1]; 
				}
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
	 * @see mapserver.Datastore#insert_value_in_table(java.io.DataInputStream)
	 */
	@Override
	public byte[] insert_value_in_table(DataInputStream dis) throws Exception{
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 final byte[] key = readNextByteArray(dis);
			 @SuppressWarnings("unchecked")
			Map<String,byte[]> value = (Map<String, byte[]>) MapSmart.deserialize(ByteStreams.toByteArray(dis));
			 Map<ByteArrayWrapper, Map<String,byte[]>> table = datastore.get(tableName);
			 MapSmart.serialize(table.put(new ByteArrayWrapper(key), value)); 
			 return new byte[1]; 
		 }
		 return null;
	}

	//XXX - create an hashmap wich takes byte wrappers. 
	
	/* (non-Javadoc)
	 * @see mapserver.Datastore#values(java.io.DataInputStream)
	 */
	@Override
	public byte[] values(DataInputStream msg) throws Exception {
		String tableName = msg.readUTF();
		if (datastore.containsKey(tableName)){
			//FIXME : get smart to check this return value with unordered maps. 
			Map<ByteArrayWrapper , Map<String,byte[]>> table = datastore.get(tableName);
			List<ByteArrayWrapper> sortedKeys= Lists.newArrayList(table.keySet());
			Collections.sort(sortedKeys, ByteArrayWrapper.COMPARE);
			List<Map<String,byte[]>> values = new ArrayList<Map<String,byte[]>>(sortedKeys.size());
			for (ByteArrayWrapper k : sortedKeys){
				values.add(new TreeMap<String,byte[]>(table.get(k)));
			}
			
			return MapSmart.serialize(values); 
		}
		return null; 
	}
	
	private Map<String,String> pointers;
	public static final byte[] TRUE = new byte[0]; 
	
	@Override 
	public byte[] create_pointer_table(DataInputStream dis) throws IOException{
		String tableName = dis.readUTF();
		String destinyTable = dis.readUTF();
		System.out.println("Creating pointers from :" + tableName + " to : " + destinyTable);
		if (!datastore.containsKey(tableName) && datastore.containsKey(destinyTable)){
			 pointers.put(tableName, destinyTable);
			 datastore.put(tableName, new HashMap<ByteArrayWrapper,Map<String,byte[]>>()); 
			 return TRUE;  
		 }
		 return null;
			//FIXME deleted tables and such.
	}
	
	@Override
	public byte[] get_referenced_value(DataInputStream dis) throws IOException{
		String tableName = dis.readUTF();
		System.out.println("Get references:" + tableName);
		if (pointers.containsKey(tableName)){
			System.out.println("In pointers" );
			ByteArrayWrapper key = new ByteArrayWrapper(readNextByteArray(dis));
			Map<ByteArrayWrapper, Map<String,byte[]>> keysTable = datastore.get(tableName);
			if (keysTable.containsKey(key)){
				System.out.println("I have the key" );
				Map<ByteArrayWrapper, Map<String,byte[]>> endTable = datastore.get(pointers.get(tableName));
				return MapSmart.serialize(endTable.get(keysTable.get(key))); 
			}
		}
		return null; 
	}
	
	@Override
	public byte[] get_column_by_reference(DataInputStream dis) throws IOException {
		
		String tableName;
		tableName = dis.readUTF();
		
		 if (pointers.containsKey(tableName)){
			 ByteArrayWrapper key = new ByteArrayWrapper(readNextByteArray(dis));
				Map<ByteArrayWrapper, Map<String,byte[]>> keysTable = datastore.get(tableName);
				if (keysTable.containsKey(key)){
					Map<ByteArrayWrapper, Map<String,byte[]>> endTable = datastore.get(pointers.get(tableName));
					String columnName = dis.readUTF();
					return endTable.get(keysTable.get(key)).get(columnName); 
				}
		 }
		 return null;
	}
	//FIXME delete referenced values. 
}
