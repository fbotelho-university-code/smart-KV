/**
 * 
 */
package smartkv.client.tables;

import net.floodlightcontroller.devicemanager.internal.Entity;
import net.floodlightcontroller.devicemanager.internal.IndexedEntity;

import com.google.common.collect.ImmutableMap;


/**
 * @author fabiim
 *
 */
public class CachedColumnTable<K,V> extends CachedKeyValueTable<K,V> implements ICachedKeyValueColumnTable<K,V>{ 
	
	
	public static <K,V> CachedColumnTable<K,V> startCache(IColumnTable<K,V> table){
		return new CachedColumnTable<K,V>(table);
	}
	
	protected CachedColumnTable(IColumnTable<K, V> table) {
		super(table,false);
		this.table = table;
		this.columnsSerializer = table.getColumnsSerializer(); 
	}
	
	IColumnTable<K,V> table;
	ColumnObject<V> columnsSerializer; 
	public <C> C getColumn(K key, String columnName, long ts){
		V value = null; 
		ClockTimeStampValue<V> vTs = cache.get(key);
		if (vTs != null && (System.currentTimeMillis() - vTs.timestamp) <= ts){ 
				value = vTs.value.value;
				return columnsSerializer.getColumn(value, columnName);
		}
		else{
			C c  = table.getColumn(key, columnName);
			V incompleteValue = columnsSerializer.fromColumns(ImmutableMap.<String, byte[]>of(columnName, columnsSerializer.serializeColumn(columnName, c)));
			cache.put(key, new ClockTimeStampValue<V>(new VersionedValue<V>(-1, incompleteValue)));
			return c; 
		}
	}
	
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IColumnTable#getColumn(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumn(K key, String columnName) {
		C c  = table.getColumn(key, columnName);
		V incompleteValue = columnsSerializer.fromColumns(ImmutableMap.<String, byte[]>of(columnName, columnsSerializer.serializeColumn(columnName, c)));
		cache.put(key, new ClockTimeStampValue<V>(new VersionedValue<V>(-1, incompleteValue)));
		return c; 
	}
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IColumnTable#getColumnByReference(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumnByReference(K key, String columnName) {
		return table.getColumn(key, columnName);
	}
	/* (non-Javadoc)
	 * @see smartkv.client.tables.IColumnTable#setColumn(java.lang.Object, java.lang.String, java.lang.Object)
	 */
	@Override
	public boolean setColumn(K key, String columnName, Object type) {
		boolean result = table.setColumn(key, columnName, type);
		if (result){
			Object c = type;  
			V incompleteValue = columnsSerializer.fromColumns(ImmutableMap.<String, byte[]>of(columnName, columnsSerializer.serializeColumn(columnName, c)));
			cache.put(key, new ClockTimeStampValue<V>(new VersionedValue<V>(-1, incompleteValue)));
		}
		return false; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IColumnTable#getColumnsSerializer()
	 */
	@Override
	public ColumnObject<V> getColumnsSerializer() {
		return columnsSerializer; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IColumnTable#replaceColumn(java.lang.Object, int, java.lang.String, byte[])
	 */
	@Override
	public boolean replaceColumn(K key, int currentValue, String columnName,
			Object value) {
		//FIXME - aproveitar valor retornado
		return table.replaceColumn(key,currentValue, columnName, value);
	}

	
	
}
