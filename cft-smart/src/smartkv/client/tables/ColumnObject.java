/**
 * 
 */
package smartkv.client.tables;

import java.util.Map;
import java.util.TreeMap;

/**
 * @author fabiim
 *
 */
public interface ColumnObject<T> {
	public TreeMap<String,byte[]> toColumns(T type); 
	public  T fromColumns(Map<String,byte[]> fields);
	public <C> C getColumn(T t , String columnName); 
	public byte[] serializeColumn(String columnName,Object val );
	public Object deserializeColumn(String columnName, byte[] val);
}

