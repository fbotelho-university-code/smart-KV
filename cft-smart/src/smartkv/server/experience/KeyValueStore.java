/**
 * 
 */
package smartkv.server.experience;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import smartkv.server.experience.values.ByteArrayKey;
import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;
import smartkv.server.experience.values.VersionedValue;
import smartkv.server.util.LRULinkedHashMap;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
public class KeyValueStore implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	protected final Map<String, Map<Key, Value>> datastore;
	protected final Map<String, Integer> counters;
	protected final Map<String, String> pointers;
	protected final KeyColumnValueStore columnStore; 
	protected final boolean keepTimeStamps;
	
	public KeyValueStore(boolean keepTimeStamps, KeyColumnValueStore columnStore){
		 datastore = new HashMap<String, Map<Key,Value>>(); 
		 pointers = Maps.newHashMap();
		 counters = Maps.newHashMap(); 
		 this.columnStore = columnStore; 
		 this.keepTimeStamps = keepTimeStamps; 
	}
	
	public Integer get_and_increment(String key) {
		Integer value =  counters.containsKey(key) ? counters.get(key) : 0;  
		counters.put(key, value +1); 
		return value; 
	}
		
	
	public Value create_table(String tableName){

		 if (!datastore.containsKey(tableName)){
			 datastore.put(tableName, keepTimeStamps ?  new VersionMap() : new NonNullValueMap());
			 return Value.TRUE;  
		 }
		 return Value.FALSE;
	}

	public Value create_pointer_table(String tableName, String destinyTable) {

		if (!datastore.containsKey(tableName) && datastore.containsKey(destinyTable)){
			 pointers.put(tableName, destinyTable);
			 datastore.put(tableName,  keepTimeStamps ? new VersionMap() : new NonNullValueMap()); 
			 return Value.TRUE;  
		 }
		else if (!datastore.containsKey(tableName) && columnStore.contains_table(destinyTable).equals(Value.TRUE)){
			 pointers.put(tableName, destinyTable);
			 datastore.put(tableName,  keepTimeStamps ? new VersionMap() : new NonNullValueMap()); 
			 return Value.TRUE;
		}
		return Value.FALSE; 
	}
	
	
	public Value get_referenced_value(String tableName, Key key) throws IOException {

		if (pointers.containsKey(tableName)){
			
			Map<Key,Value> keysTable = datastore.get(tableName);
			if (keysTable.containsKey(key)){
			
				Map<Key,Value> endTable = datastore.get(pointers.get(tableName));
				if (endTable != null){
			
					Value referencedValue = endTable.get(createKeyFromByteArray(keysTable.get(key)));
			
					return referencedValue;
				}
				else if (columnStore.contains_table(pointers.get(tableName)).equals(Value.TRUE)){
					Value v =  columnStore.get_value_in_table(pointers.get(tableName),createKeyFromByteArray(keysTable.get(key)));
					
					return v; 
				}
			}
		}

		return Value.SingletonValues.EMPTY; 
	}
	
	public Value create_table_max_size(String tableName, int maxSize){
		if (!datastore.containsKey(tableName)){
			 datastore.put(tableName, new LRULinkedHashMap<Key,Value>(maxSize)); 
			 return Value.TRUE; 
		 }
		return Value.FALSE; 
	}

	public Value remove_table(String tableName)  {
		 if (datastore.containsKey(tableName)){
			 datastore.remove(tableName); //TODO - remove hack
			 if (pointers.containsKey(tableName)){
				 pointers.remove(tableName);
			 }
			 //FIXME we should not traverse every pointer table and delete references to deleted tables. 
			 return Value.TRUE; 
		 }
		 return Value.FALSE;
	}
	
	
	public Value contains_table(String needle) {
		if (datastore.containsKey(needle)){
			 return Value.TRUE;  
		}
		return Value.FALSE;
	}
	
	public Value clear_table(String tableName)  {
		 if (datastore.containsKey(tableName)){
			 Map<?,?> m = datastore.get(tableName); 
			 if (m != null){
				 m.clear(); 
				 return Value.TRUE;  
			 }
		 }
		 return Value.FALSE;
	}

	
	public Value contains_key_in_table(String tableName, Key key) {
		 if (datastore.containsKey(tableName)){
			 Map<?,?> tableHayStack = datastore.get(tableName);
			 if (tableHayStack.containsKey(key)){
				 return Value.TRUE;  
			 }
		 }
		return Value.FALSE; 
	}
	
	
	public Value get_value_in_table(String tableName, Key key) throws IOException {
		 if (datastore.containsKey(tableName)){
			 return datastore.get(tableName).get(key); 
		 }
		 return Value.SingletonValues.EMPTY; 
	}

	
	public Value is_datastore_empty() {
		if (datastore.isEmpty()){
			 return Value.TRUE;  
		 }
		return Value.FALSE; 
	}

	
	public Value is_table_empty(String tableName) throws IOException {

		 if (datastore.containsKey(tableName) ){
			 Map<?,?> table = datastore.get(tableName); 
			 if (table.isEmpty()){
				 return Value.TRUE;  
			 }
		 }
		 return Value.SingletonValues.EMPTY; 
	}
	
	
	public Value  put_value(String tableName, Key key, Value val){
		Value v  = put_value_and_get_previous(tableName, key, val);
		return v != Value.FALSE ?  Value.TRUE : Value.FALSE; 
		
		//return v != Value.SingletonValues.EMPTY ? Value.TRUE : Value.FALSE; 
	}
	
	public Value put_value_and_get_previous(String tableName, Key key, Value val){
		 if (datastore.containsKey(tableName)){
			 Map<Key,Value> table = datastore.get(tableName);
			 Value v = table.put(key, val);
			 return v; 
		 }
		 return Value.FALSE; 
	}
	
	public Value remove_value_from_table(String tableName, Key key){
		if (datastore.containsKey(tableName)){
			 Map<Key,Value> table = datastore.get(tableName);
			 if (table.containsKey(key)){
				 return table.remove(key);
			 }
		 }
		 return Value.SingletonValues.EMPTY;
	}

	public Value atomic_replace_value_in_table(String tableName, Key k, Value expectedValue, Value newValue){
		 if (datastore.containsKey(tableName)){
			 Map<Key,Value> table = datastore.get(tableName);
			 if (table.containsKey(k)){
				 if (table.get(k).equals(expectedValue)){
					 table.put(k, newValue);
					 return Value.TRUE;  
				 }
			 }
		 }
		 return Value.SingletonValues.FALSE;
	}
	
	/**
	 * @param tableName
	 * @param k
	 * @param expectedVersion
	 * @param newValue
	 * @return
	 */
	public Value atomic_replace_value_in_table(String tableName, Key k,
			int expectedVersion, Value newValue) {
		 if (datastore.containsKey(tableName)){
			 Map<Key,Value> table = datastore.get(tableName);
			 if (table.containsKey(k)){
				 VersionedValue v = (VersionedValue) table.get(k);
				 if (v.getVersion() == (expectedVersion)){
					 table.put(k, newValue);
					 return Value.TRUE;  
				 }
			 }
		 }
		 return Value.SingletonValues.FALSE;
	}
	
	public Value atomic_remove_if_value(String tableName, Key key , Value expectedValue){
		 if (datastore.containsKey(tableName)){
			 Map<Key,Value> table = datastore.get(tableName);
			 if (table.containsKey(key)){
				 if (table.get(key).equals(expectedValue)){
					 table.remove(key); 
					 return Value.TRUE; 
				 }
			 }
		 }
		 return Value.SingletonValues.FALSE;
	}

	
	
	public Value atomic_put_if_absent(String tableName, Key key, Value value){
		if (datastore.containsKey(tableName)){
			Map<Key,Value> table = datastore.get(tableName);
			if (!table.containsKey(key)){
				return table.put(key, value);
			}
			else{
				return table.get(key);
			}
		}
		return Value.FALSE; 
	}

	public Value clear()  {
		datastore.clear();
		return Value.TRUE;
	}
	
	public Collection<Value> values(String tableName)  {
		Collection<Entry<Key,Value>> entries = get_table(tableName);
		List<Value> val = Lists.newArrayList(); 
		if (entries != null){
			for (Entry<Key,Value> en : entries){
				val.add(en.getValue());
			}
			return val; 
		}
		return null; 
	}
	
	public Collection<Entry<Key, Value>> get_table(String tableName){
		if (datastore.containsKey(tableName)){
			List<Entry<Key, Value>> list = Lists.newArrayList(datastore.get(tableName).entrySet()); 
			Collections.sort(list, new Comparator<Entry<Key,Value>>(){
				@Override
				public int compare (Entry<Key,Value> e1, Entry<Key,Value> e2){
					return e1.getKey().compareTo(e2.getKey()); 
				}
			});
			return list; 
		}
		return null; 
	}
	
	private Key createKeyFromByteArray(Value value) {
		if (!keepTimeStamps){
			return new ByteArrayKey(value.asByteArray());
		}
		if (value instanceof VersionedValue){
			return new ByteArrayKey(((VersionedValue ) value).getValue().asByteArray()); 
		}
		return new ByteArrayKey(value.asByteArray()); 
	}

	/**
	 * @param tableName
	 * @return
	 */
	public int size_of_table(String tableName) {
		return datastore.containsKey(tableName) ? datastore.get(tableName).size() : 0;  
	}

	/**
	 * @param tableName
	 * @param key
	 * @param columns
	 * @return
	 * @throws IOException 
	 */
	public Value get_referenced_columns_value(String tableName, Key key,
			Set<String> columns) throws IOException {
		Value v = this.get_value_in_table(tableName, key);
		if (v != Value.SingletonValues.EMPTY){
			
			Value vv =  this.columnStore.get_columns(pointers.get(tableName),createKeyFromByteArray(v), columns);
			return vv; 
		}
		return Value.SingletonValues.EMPTY; 
	}
}
