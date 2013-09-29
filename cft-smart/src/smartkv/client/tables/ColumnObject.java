/**
 * 
 */
package smartkv.client.tables;

import java.util.Map;

/**
 * @author fabiim
 *
 */
public interface ColumnObject<T> {
	public Map<String,byte[]> toColumns(T type); 
	public  T fromColumns(Map<String,byte[]> fields);
	public byte[] serializeColumn(String columnName,Object val );
	public Object deserializeColumn(String columnName, byte[] val);
}

