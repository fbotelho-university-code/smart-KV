/**
 * 
 */
package smartkv.datastore.workloads;

import smartkv.datastore.ColumnProxy;
import smartkv.datastore.tables.ColumnObject;
import smartkv.datastore.tables.ColumnTable;
import smartkv.datastore.tables.ColumnTable_;
import smartkv.datastore.util.Serializer;
import smartkv.datastore.util.UnsafeJavaSerializer;
import mapserver.RequestType;

public class ColumnWorkloadLogger< K,V> extends WorkloadLoggerTable<K,V> implements ColumnTable<K,V>{

	
	ColumnTable<K,V> table2; 
	
	public ColumnWorkloadLogger(String tableName, RequestLogger logger, ColumnObject<V> valueSerializer){
		this(tableName, logger, UnsafeJavaSerializer.<K>getInstance(), valueSerializer); 
	}
	
	
	public ColumnWorkloadLogger(String tableName, RequestLogger logger, Serializer<K> keys, ColumnObject<V> values){
		this.entry = new RequestLogEntry(); 
		this.logger = logger;
		this.tableName = tableName; 
		this.table =this.table2 = ColumnTable_.getTable( new ColumnProxy(0){
			@Override
			protected byte[] invokeRequestWithRawReturn(RequestType type, byte[] request) {
				entry.setTimeStarted(System.currentTimeMillis());
				entry.setSizeOfRequest(request.length);
				entry.setType(type);
				byte[] result =  super.invokeRequestWithRawReturn(type, request);
				entry.setTimeEnded(System.currentTimeMillis());
				return result; 
			}; 
		}
		, tableName, keys,values);
	}

	

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#getColumn(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumn(K key, String columnName) {
		C val = table2.getColumn(key, columnName); 
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null ? key.toString() : "null").
				setColumn(columnName).
				setReturnedValue(val != null ? val.toString() : "null").
				build(entry));
		return val; 
	}
	///XXX em vez de blah != null , podias adicionar o setBlah(Object o) e chamar o toString.  
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#setColumn(java.lang.Object, java.lang.String, java.lang.Object)
	 */
	@Override
	public boolean setColumn(K key, String columnName, Object type) {
		Boolean val = table2.setColumn(key, columnName, type);
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null ? key.toString() : "null").
				setColumn(columnName).
				setValue(type.toString()).
				setReturnedValue(val != null ? val.toString() : "null").
				build(entry));
		return val; 
	}


	


	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#getColumnByReference(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumnByReference(K key, String columnName) {
		C val = table2.getColumnByReference(key, columnName); 
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null ? key.toString() : "null").
				setColumn(columnName).
				setReturnedValue(val != null ? val.toString() : "null").
				build(entry));
		return val; 
	}
}