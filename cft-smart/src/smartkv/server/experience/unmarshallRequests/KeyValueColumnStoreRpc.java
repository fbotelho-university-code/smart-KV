/**
 * 
 */
package smartkv.server.experience.unmarshallRequests;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import smartkv.server.ColumnDatastore;
import smartkv.server.MapSmart;
import smartkv.server.experience.KeyColumnValueStore;
import smartkv.server.experience.values.ByteArrayValue;
import smartkv.server.experience.values.ColumnValue;
import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;

import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;


//TODO: logger prints only in one replica. 

/**
 * @author fabiim
 *
 */

//FIXME : this screams memory leak all over the place. The key, map cannot be garbage collected after it has been eliminated. 


public class KeyValueColumnStoreRpc extends KeyValueStoreRPC implements ColumnDatastore {
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	KeyColumnValueStore datastore;
	
	public KeyValueColumnStoreRpc(boolean keeptimestamps){
		super(keeptimestamps, null); 
		
		datastore = new KeyColumnValueStore(keeptimestamps);
		//paranoid java n00b warning. Too lazy to go on the internet... 
		super.datastore = this.datastore; 
	}
	
	@Override
	public byte[] get_column(DataInputStream dis) throws IOException {
		String tableName;
		tableName = dis.readUTF();
		Key key =createKeyFromBytes(readNextByteArray(dis));
		String columnName = dis.readUTF();
		return datastore.get_column(tableName,key,columnName).asByteArray(); 
	}		
	
	public byte[] get_columns(DataInputStream dis) throws IOException{
		String tableName;
		tableName = dis.readUTF();
		Key key =createKeyFromBytes(readNextByteArray(dis));
		Set<String> columns = Sets.newHashSet();
		while (dis.available() >0 ){
			String s = dis.readUTF();
			columns.add(s); 
		}
		
		return datastore.get_columns(tableName,key,columns).asByteArray();
	}
	
	/* (non-Javadoc)
	 * @see mapserver.ColumnDatastore#put_column(java.io.DataInputStream)
	 */
	@Override
	public byte[] put_column(DataInputStream dis) throws Exception{
		String tableName =dis.readUTF();
		Key key = createKeyFromBytes(readNextByteArray(dis));
		String column=  dis.readUTF();
		byte[] v =ByteStreams.toByteArray(dis); 
		System.out.println(Arrays.toString(v));
		Value value = createColumnValueFromBytes(v);
		return datastore.put_column(tableName, key, column, value).asByteArray();
	}
	
	protected Value createColumnValueFromBytes(byte[] valueBytes){
		return new ByteArrayValue(valueBytes); 
	}
	
	@Override
	public Value createValueFromBytes(byte [] valueBytes) throws IOException, ClassNotFoundException{
		@SuppressWarnings("unchecked")
		Map<String, byte[]> deserialize = (Map<String, byte[]>) MapSmart.deserialize(valueBytes);
		return ColumnValue.createSimpleValueFromMap(deserialize); 
	}
	
	@Override
	public byte[] get_column_by_reference(DataInputStream dis) throws IOException {
		throw new UnsupportedOperationException("Not yet Implemented!");
		/*String tableName;
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
		 return null;*/
	}

	/* (non-Javadoc)
	 * @see smartkv.server.ColumnDatastore#getDatastore()
	 */
	@Override
	public KeyColumnValueStore getDatastore() {
		return datastore; 
	}
	
	/* (non-Javadoc)
	 * @see smartkv.server.ColumnDatastore#get_columns(java.io.DataInputStream)
	 */
	
}
