/**
 * 
 */
package smartkv.server.experience;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import com.google.common.io.ByteStreams;

import smartkv.server.ByteArrayWrapper;
import smartkv.server.experience.values.ColumnValue;
import smartkv.server.experience.values.ByteArrayKey;
import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;

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
		if (datastore.containsKey(tableName)){
			Map<Key, Value> table = datastore.get(tableName);
			if (table.containsKey(key)){
				ColumnValue value = (ColumnValue) table.get(key);
				if (value.containsKey(columnName)){
					return value.put(columnName, columnValue);
				}
			}
		}
		return Value.FALSE; 
	}
	
	public  Value get_column(String tableName, Key key, String columnName){
		 if (datastore.containsKey(tableName) && datastore.get(tableName).containsKey(key)){
			 ColumnValue value = (ColumnValue) datastore.get(tableName).get(key); 
			 return value.get(columnName); 
		 }
		 return null;
	}
	
}
