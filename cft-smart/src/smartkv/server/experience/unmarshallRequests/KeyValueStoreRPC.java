/**
 * 
 */
package smartkv.server.experience.unmarshallRequests;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import smartkv.server.ByteArrayWrapper;
import smartkv.server.Datastore;
import smartkv.server.MapSmart;
import smartkv.server.experience.KeyValueStore;
import smartkv.server.experience.values.ByteArrayKey;
import smartkv.server.experience.values.ByteArrayValue;
import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;
import smartkv.server.util.LRULinkedHashMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Ints;
import com.google.common.primitives.UnsignedBytes;


public class KeyValueStoreRPC implements Datastore, Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected KeyValueStore datastore;
	
	/**
	 * @param keeptimestamps
	 */
	public KeyValueStoreRPC(boolean keeptimestamps) {
		datastore = new KeyValueStore(keeptimestamps); 
	}
	
	@Override
	public byte[] get_and_increment(DataInputStream dis) throws IOException, ClassNotFoundException{
		String key;
		key = dis.readUTF();
		Integer l = datastore.get_and_increment(key);
		return Ints.toByteArray(l);
	}
		
	@Override
	public byte[] create_table(DataInputStream dis) throws IOException {
		String tableName = dis.readUTF();
		return datastore.create_table(tableName).asByteArray();
	}

	@Override 
	public byte[] create_pointer_table(DataInputStream dis) throws IOException{
		String tableName = dis.readUTF();
		String destinyTable = dis.readUTF();
		return datastore.create_pointer_table(tableName, destinyTable).asByteArray();
	}
	
	@Override
	public byte[] get_referenced_value(DataInputStream dis) throws IOException{
		String tableName = dis.readUTF();
		Key key = ByteArrayKey.createKeyFromBytes(readNextByteArray(dis));
		return datastore.get_referenced_value(tableName, key).asByteArray();
	}
	
	@Override
	public byte[] create_table_max_size(DataInputStream dis)
			throws IOException {
		String tableName;
		tableName = dis.readUTF();
		int size = dis.read(); 
		return datastore.create_table_max_size(tableName, size).asByteArray();
	}

	@Override
	public byte[] remove_table(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF();
		return datastore.remove_table(tableName).asByteArray();
	}

	@Override
	public byte[] contains_table(DataInputStream dis) throws IOException {
		String needle = dis.readUTF(); 
		return datastore.contains_table(needle).asByteArray(); 
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
		return datastore.clear_table(tableName).asByteArray();
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
		Key key = ByteArrayKey.createKeyFromBytes(readNextByteArray(dis)); 
		return datastore.contains_key_in_table(tableName, key).asByteArray(); 
	}
	
	@Override
	public byte[] get_table(DataInputStream dis) throws IOException {
		throw new UnsupportedOperationException("Not yet implemented!");
		//String tableName;
		//tableName = dis.readUTF();
		/*Collection<Entry<Key, Value>> table = datastore.get_table(tableName);
		MapSmart.serialize(table); 
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
		 //return null;
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
		Key key = ByteArrayKey.createKeyFromBytes(readNextByteArray(dis));
		return datastore.get_value_in_table(tableName, key).asByteArray(); 
	}

	@Override
	public byte[] is_table_empty(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF();
		return datastore.is_table_empty(tableName).asByteArray();
	}
	
	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] put_value_in_table(
			DataInputStream dis) throws Exception {
		String tableName;
		tableName = dis.readUTF();
		Key key = createKeyFromBytes(readNextByteArray(dis));
		Value value = createValueFromBytes(ByteStreams.toByteArray(dis));
		return datastore.put_value_and_get_previous(tableName, key, value).asByteArray(); 
	}

	/**
	 * @param readNextByteArray
	 * @return
	 */
	protected Key createKeyFromBytes(byte[] readNextByteArray) {
		return  ByteArrayKey.createKeyFromBytes(readNextByteArray); 
	}

	protected Value createValueFromBytes(byte[] readValue) throws IOException, ClassNotFoundException{
	 	return ByteArrayValue.createValueFromByteArray(readValue); 
	}
	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] put_Values_in_table(DataInputStream dis) throws Exception {
		throw new UnsupportedOperationException("not yet implemented"); 
		//String tableName;
		
/*		tableName =dis.readUTF(); 
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
		 //return null;
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
			Key key = createKeyFromBytes(readNextByteArray(dis));
			return datastore.remove_value_from_table(tableName, key).asByteArray(); 
	}

	
	@Override
	public byte[] size_of_table(DataInputStream dis) throws IOException {
		String tableName = dis.readUTF();
		return Ints.toByteArray(datastore.size_of_table(tableName)); 
	}
	
	
	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] atomic_replace_value_in_table(
			DataInputStream dis) throws Exception {
		String tableName = dis.readUTF(); 
		Key k = createKeyFromBytes(readNextByteArray(dis));
		Value oldValue = createValueFromBytes(readNextByteArray(dis));
		Value newValue = createValueFromBytes(ByteStreams.toByteArray(dis)); 
		return datastore.atomic_replace_value_in_table(tableName, k, oldValue, newValue).asByteArray(); 
	}
	
	@Override
	public byte[] atomic_replace_value_in_table_with_timestamp(DataInputStream dis) throws Exception{
		String tableName = dis.readUTF(); 
		Key k = createKeyFromBytes(readNextByteArray(dis));
		byte[] intBytes = new byte[4]; 
		dis.readFully(intBytes);
		Integer expectedVersion = Ints.fromByteArray(intBytes);
		Value newValue = createValueFromBytes(ByteStreams.toByteArray(dis));
		return datastore.atomic_replace_value_in_table(tableName, k,expectedVersion, newValue).asByteArray(); 
	}
	
	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	@Override
	public byte[] atomic_remove_if_value(
			DataInputStream dis) throws Exception {
		String tableName = dis.readUTF();
		Key  key = createKeyFromBytes(readNextByteArray(dis));
		Value currValue = createValueFromBytes(ByteStreams.toByteArray(dis));
		return datastore.atomic_remove_if_value(tableName, key, currValue).asByteArray(); 
	}
	
	/**
	 * @param in
	 * @param dis
	 * @return 
	 * @throws IOException
	 */
	@Override
	public byte[] atomic_put_if_absent(
			DataInputStream dis) throws Exception {
		String tableName;
		Key key;
		tableName = dis.readUTF(); 
		key=createKeyFromBytes(readNextByteArray(dis));
		Value value = createValueFromBytes( ByteStreams.toByteArray(dis));
		return datastore.atomic_put_if_absent(tableName, key, value).asByteArray();
		
	}
	
	protected byte[] readNextByteArray(DataInputStream in) throws IOException {
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
	//XXX - it would be cool if the message just had a bit saying the kind of return it want it. 
	public byte[] insert_value_in_table(DataInputStream dis) throws Exception{
		String tableName;
		tableName = dis.readUTF();
		Key key = createKeyFromBytes(readNextByteArray(dis)); 
		Value value = createValueFromBytes(ByteStreams.toByteArray(dis));
		return datastore.put_value(tableName, key, value).asByteArray();
	}
	
	/* (non-Javadoc)
	 * @see mapserver.Datastore#values(java.io.DataInputStream)
	 */
	@Override
	public byte[] values(DataInputStream msg) throws Exception {
		String tableName = msg.readUTF();
		Collection<Value> values = datastore.values(tableName);
		if (values != null){
			Collection<byte[]> valuesAsBytes = Lists.newArrayList();
			for (Value v : values){
				valuesAsBytes.add(v.asByteArray());
			}
			return MapSmart.serialize(valuesAsBytes);
		}
		else return null; 
	}

	/* (non-Javadoc)
	 * @see smartkv.server.Datastore#is_datastore_empty()
	 */
	@Override
	public byte[] is_datastore_empty() throws Exception {
		return datastore.is_datastore_empty().asByteArray(); 
	}

	
}
