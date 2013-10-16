/**
 * 
 */
package smartkv.server.experience;

import java.util.Map;

import smartkv.server.experience.values.ColumnValue;
import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;
import smartkv.server.experience.values.VersionedValue;

/**
 * @author fabiim
 *
 */
public class KeyColumnValueStore extends KeyValueStore{
	/**
	 * @param keepTimeStamps
	 */
	public KeyColumnValueStore(boolean keepTimeStamps) {
		super(keepTimeStamps);
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
	
}
