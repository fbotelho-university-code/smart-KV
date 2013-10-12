/**
 * 
 */
package smartkv.server.experience.values;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.SortedMap;

import smartkv.server.MapSmart;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * @author fabiim
 *
 */
public class ColumnValue implements Value{
	private final SortedMap<String,Value> columns;
	
	public static Value createSimpleValueFromMap(Map<String,byte[]> map){
		SortedMap<String,Value> newMap =  Maps.newTreeMap();
		for (Entry<String, byte[]> entry : map.entrySet()){
			newMap.put(entry.getKey(), ByteArrayValue.createValueFromByteArray(entry.getValue())); 
		}
		return new ColumnValue(newMap); 
	}
	
	public ColumnValue(SortedMap<String, Value> columns) {
		this.columns = columns; 
	}
	
	/**
	 * @param columnName
	 * @return
	 */
	public Value get(String columnName) {
		Value val = columns.get(columnName); 
		return val != null ? val : Value.SingletonValues.EMPTY; 
	}
	
	public boolean containsKey(String columnName) {
		return columns.containsKey(columnName); 
	}
	
	
	/**
	 * @param columnName
	 * @param columnValue
	 * @return
	 */
	public Value put(String columnName, Value columnValue) {
		return columns.put(columnName, columnValue);
	}
	
	
	@Override
	public byte[] asByteArray() {
		ImmutableMap.Builder<String, byte[]> result = ImmutableMap.builder();
		for ( Entry<String, Value> i : columns.entrySet()){
			result.put(i.getKey(), i.getValue().asByteArray());  
		}
		try {
			return MapSmart.serialize(result.build());
		} catch (IOException e) {
			//XXX get a way to serialize without exceptions. 
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((columns == null) ? 0 : columns.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		ColumnValue other = (ColumnValue) obj;
		if (columns == null) {
			if (other.columns != null)
				return false;
		} else if (!columns.equals(other.columns))
			return false;
		return true;
	}
	
	@Override
	public void arrangeDataDeterministically() {
		// TODO Auto-generated method stub
		
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

}