/**
 * 
 */
package smartkv.server.experience;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import smartkv.server.experience.values.ByteArrayValue;
import smartkv.server.experience.values.ColumnValue;
import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;
import smartkv.server.experience.values.VersionedValue;

public class KeyColumnValueStore extends KeyValueStore{
	/**
	 * @param keepTimeStamps
	 */
	public KeyColumnValueStore(boolean keepTimeStamps) {
		super(keepTimeStamps, null);
	}

	private static final long serialVersionUID = 1L;
	
	public Value get_column_by_reference(String tableName, Key key, String columnName) {
		 if (pointers.containsKey(tableName)){
				Map<Key,Value> keysTable = datastore.get(tableName);
				if (keysTable.containsKey(key)){
					Map<Key, Value> endTable = datastore.get(pointers.get(tableName));
					return ((ColumnValue) endTable.get(keysTable.get(key))).get(columnName); 
				}
		 }
		 return null;
	}
	
	public Value put_column(String tableName, Key key, String columnName, Value columnValue){
			if (datastore.containsKey(tableName) && datastore.get(tableName).containsKey(key)){
				ColumnValue value =  getColumnValue(tableName, key);
				if (value.containsKey(columnName)){
					return value.put(columnName, columnValue);
				}
			}
		return Value.FALSE; 
	}
	
	public  Value get_column(String tableName, Key key, String columnName){
		 if (datastore.containsKey(tableName) && datastore.get(tableName).containsKey(key)){
			 ColumnValue value = getColumnValue(tableName, key);  
			 return value.get(columnName); 
		 }
		 return null;
	}
	
	private ColumnValue getColumnValue(String tableName, Key key){
		return !this.keepTimeStamps ? (ColumnValue) datastore.get(tableName).get(key) :(ColumnValue) ((VersionedValue) datastore.get(tableName).get(key)).getValue(); 
	}
	
	/**
	 * @param tableName
	 * @param key
	 * @param columns
	 * @return
	 * @throws IOException 
	 */
	public Value get_columns(String tableName, Key key,
			Set<String> columns) throws IOException {
		Value s = get_value_in_table(tableName, key);
		System.out.println(s);		
		if (this.keepTimeStamps && !(s instanceof Value.SingletonValues)){
			VersionedValue vs = (VersionedValue) s;
			VersionedValue  newVs = new VersionedValue(new ColumnValue((ColumnValue) vs.getValue(), columns), vs.getVersion());
			return newVs; 
		}
		else {
			return s; 
		}
		
	}

	/**
	 * @param table
	 * @param key
	 * @param column
	 * @param v
	 * @return
	 * @throws IOException 
	 */
	public Value replace_column(String tableName, Key key, int knownVersion,String columnName,
			Value columnValue) throws IOException {
		if (datastore.containsKey(tableName) && datastore.get(tableName).containsKey(key)){
			VersionedValue v = (VersionedValue) super.get_value_in_table(tableName, key);
			if (v.getVersion()  == knownVersion){
				ColumnValue value = getColumnValue(tableName, key);
				if (value.containsKey(columnName)){
					value.put(columnName, columnValue);
					return Value.TRUE; 
				}
			}
		}
		return Value.FALSE;
	}
	
}
