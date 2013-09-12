package mapserver;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import mapserver.util.LRULinkedHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bftsmart.tom.MessageContext;
import bftsmart.tom.ReplicaContext;
import bftsmart.tom.ServiceReplica;
import bftsmart.tom.server.defaultservices.DefaultSingleRecoverable;

import com.google.common.collect.Maps;
import com.google.common.collect.Table;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.UnsignedBytes;

class ByteArrayWrapper implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public final byte[] value;

	//TODO - change this for something that can be serializable?
	public static final HashFunction hf = Hashing.murmur3_32();
	
	
	public ByteArrayWrapper(byte[] v){
		value = v; 
	}



	@Override
	public final boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ByteArrayWrapper other = (ByteArrayWrapper) obj;
		if (!Arrays.equals(value, other.value))
			return false;
		return true;
	}
	
	@Override
	public  final
	int hashCode(){
		return hf.hashBytes(value).asInt(); 
		
	}

}

public class MapSmart extends DefaultSingleRecoverable{
	
	//TODO extract these methods to a standalone package. 
	public static byte[] serialize(Object obj) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        ObjectOutputStream o = new ObjectOutputStream(b);
        o.writeObject(obj);
        o.flush(); 
        o.close(); 
        return b.toByteArray();
    }
	
    public static Object deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
        ByteArrayInputStream b = new ByteArrayInputStream(bytes);
        ObjectInputStream o = new ObjectInputStream(b);
        return o.readObject();
    }
	
	public static void main(String[] args){
		new MapSmart(0);
		new MapSmart(1);
		new MapSmart(2);
		new MapSmart(3);
		
		File f = new File("./config/currentView");
		if (f.exists()){
			f.delete();
		}
	}
	
	 ServiceReplica replica = null;
     @SuppressWarnings("unused")
     private ReplicaContext replicaContext;
     private Map<String, Map<ByteArrayWrapper, byte[]>> datastore;
     
     private Logger log = LoggerFactory.getLogger(MapSmart.class);
     
     public MapSmart(int id) {
             replica = new ServiceReplica(id, this, this);
             datastore = new HashMap<String, Map<ByteArrayWrapper,byte[]>>();
     }
     
	@Override
	public void setReplicaContext(ReplicaContext replicaContext) {
		this.replicaContext = replicaContext;
	}
	
	@Override
	public byte[] executeUnordered(byte[] command, MessageContext msgCtx) {
	 return null;    
	}
	
	@Override
	public byte[] appExecuteOrdered(byte[] command, MessageContext msgCtx) {
		 ByteArrayInputStream in = new ByteArrayInputStream(command);
		 try {
			 DataInputStream dis = new DataInputStream(in); 
			 DataStoreVersion 
			 RequestType reqType = RequestType.values()[dis.readInt()];
			 switch(reqType){
			 
			 case GET_AND_INCREMENT:
				 return get_and_increment(dis); 
			 case CREATE_TABLE:
				 return create_table(dis);
			 case CREATE_TABLE_MAX_SIZE: 
				 return create_table_max_size(dis); 
			 case REMOVE_TABLE: 
				 return remove_table(dis); 
			 case CONTAINS_TABLE: 
				 return contains_table(dis); 
			 case CLEAR_DATASTORE: 
				 datastore.clear(); 
				 return null; 
			 case CLEAR_TABLE: 
				 return clear_table(dis); 
			 case CONTAINS_KEY_IN_TABLE: 
				 return contains_key_in_table(in, dis);
			 case GET_TABLE: 
				 return get_table(dis);
			 case GET_VALUE_IN_TABLE: 
				 return get_value_in_table(in, dis); 
			 case IS_DATASTORE_EMPTY: 
				 return is_datastore_empty();
			 case IS_TABLE_EMPTY: 
				 return is_table_empty(dis);
			 case PUT_VALUE_IN_TABLE:
				 return put_value_in_table(in, dis); 
			 case PUT_VALUES_IN_TABLE: 
				 return put_Values_in_table(dis,in);
			 case REMOVE_VALUE_FROM_TABLE: 
				 return remove_value_from_table(in, dis);
			 case SIZE_OF_TABLE: 
				 return size_of_table(dis);
				 //TODO default. 
			 case ATOMIC_REPLACE_VALUE_IN_TABLE:
				return atomic_replace_value_in_table(in, dis);
			 case ATOMIC_REMOVE_IF_VALUE: 
				return atomic_remove_if_value(in, dis);
		    case ATOMIC_PUT_IF_ABSENT: 
				return atomic_put_if_absent(in, dis);
			 }
		 } catch (IOException e) {
			 System.err.println("Exception reading data in the replica: " + e.getMessage());
			 e.printStackTrace();
			 return null;
		 } catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 return null; 
	}

	//FIXME
	private byte[] get_and_increment(DataInputStream dis) throws IOException, ClassNotFoundException{
		String tableName;
		tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 byte[] key =readNextByteArray(dis);
			 byte[] val = datastore.get(tableName).get(new ByteArrayWrapper(key));
			 if (val != null){
				 
			 Long l =(Long) MapSmart.deserialize(val);  
			 System.out.println(l);
			 
			 datastore.get(tableName).put(new ByteArrayWrapper(key), MapSmart.serialize(l +1));
			 return MapSmart.serialize(l);
			 }
		 }
		 return null; 
	}

	
	

	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	private byte[] create_table(DataInputStream dis) throws IOException {
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
	private byte[] create_table_max_size(DataInputStream dis)
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
	private byte[] remove_table(DataInputStream dis) throws IOException {
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
	private byte[] contains_table(DataInputStream dis) throws IOException {
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
	private byte[] clear_table(DataInputStream dis) throws IOException {
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
	private byte[] contains_key_in_table(ByteArrayInputStream in,
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
	private byte[] get_table(DataInputStream dis) throws IOException {
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
	private byte[] get_value_in_table(ByteArrayInputStream in,
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
	private byte[] is_datastore_empty() {
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
	private byte[] is_table_empty(DataInputStream dis) throws IOException {
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
	private byte[] put_value_in_table(ByteArrayInputStream in,
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

	private byte[] insert_value_in_table(ByteArrayInputStream in,
			DataInputStream dis) throws IOException {
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
	
	/**
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	private byte[] put_Values_in_table(DataInputStream dis, ByteArrayInputStream in) throws IOException {
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
					 log.error("Could not deserialize the map of key-value pairs entries to add to table: " + tableName + e.getStackTrace());
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
	private byte[] remove_value_from_table(ByteArrayInputStream in,
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
	private byte[] size_of_table(DataInputStream dis) throws IOException {
		String tableName = dis.readUTF(); 
		 if (datastore.containsKey(tableName)){
			 return toBytes(datastore.get(tableName).size()); 
		 }
		 return null;
	}

	/**
	 * @param in
	 * @param dis
	 * @return
	 * @throws IOException
	 */
	private byte[] atomic_replace_value_in_table(ByteArrayInputStream in,
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
	private byte[] atomic_remove_if_value(ByteArrayInputStream in,
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
	private byte[] atomic_put_if_absent(ByteArrayInputStream in,
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

	@SuppressWarnings("unchecked")
	@Override
	public void installSnapshot(byte[] state) {
		 ByteArrayInputStream bis = new ByteArrayInputStream(state);
         try {
                 ObjectInputStream in = new ObjectInputStream(bis);
                 datastore = (Map<String,Map<ByteArrayWrapper, byte[]>>) in.readObject();
                 in.close();
                 bis.close();
         } catch (ClassNotFoundException e) {
                 System.err.print("Coudn't find Map: " + e.getMessage());
                 e.printStackTrace();
         } catch (IOException e) {
                 System.err.print("Exception installing the application state: " + e.getMessage());
                 e.printStackTrace();
         }
		
	}

	@Override
	public byte[] getSnapshot() {
		 try {
             ByteArrayOutputStream bos = new ByteArrayOutputStream();
             ObjectOutputStream out = new ObjectOutputStream(bos);
             out.writeObject(datastore);
             out.flush();
             out.close();
             bos.close();
             return bos.toByteArray();
     } catch (IOException e) {
             System.out.println("Exception when trying to take a + " +
                             "snapshot of the application state" + e.getMessage());
             e.printStackTrace();
             return new byte[0];
     }	// TODO Auto-generated method stub
	}

	
	
	private byte[] toBytes(int i) {
		byte[] result = new byte[4];
		result[0] = (byte) (i >> 24);
		result[1] = (byte) (i >> 16);
		result[2] = (byte) (i >> 8);
		result[3] = (byte) (i);
		return result;
	}
}
