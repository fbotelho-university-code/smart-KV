/**
 * 
 */
package smartkv.client.workloads;

import smartkv.client.ColumnProxy;
import smartkv.client.tables.ColumnObject;
import smartkv.client.tables.ColumnTable_;
import smartkv.client.tables.IColumnTable;
import smartkv.client.tables.TableBuilder;
import smartkv.server.RequestType;

public class ColumnWorkloadLogger< K,V> extends WorkloadLoggerTable<K,V> implements IColumnTable<K,V>{

	
	IColumnTable<K,V> table;
	
	
	public static <K,V> ColumnWorkloadLogger<K,V> withSingletonLogger(TableBuilder<K,V> builder){
		 return new ColumnWorkloadLogger<K,V>(builder, RequestLogger.getRequestLogger()); 
	}
	
	public ColumnWorkloadLogger(TableBuilder<K,V> builder,RequestLogger logger){
		super();
		super.entry = new RequestLogEntry();  
		super.logger = logger;
		super.tableName =builder.getTableName();
		if (builder.getProxy() ==null){
		builder.setProxy(new ColumnProxy(builder.getCid()){
			@Override
			protected byte[] invokeRequestWithRawReturn(RequestType type, byte[] request) {
				entry.setTimeStarted(System.currentTimeMillis());
				entry.setSizeOfRequest(request.length);
				entry.setType(type);
				byte[] result =  super.invokeRequestWithRawReturn(type, request);
				entry.setTimeEnded(System.currentTimeMillis());
				entry.setSizeOfResponse( result != null ? result.length : 0);
				return result; 
			}
		});
		}
		this.table = new ColumnTable_<K,V>(builder);
		super.table = this.table; 
	}

	

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnTable#getColumn(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumn(K key, String columnName) {
		C val = table.getColumn(key, columnName); 
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
		Boolean val = table.setColumn(key, columnName, type);
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
		C val = table.getColumnByReference(key, columnName); 
		logEntry(new RequestLogWithDataInformation.Builder().
				setTable(tableName).
				setKey(key != null ? key.toString() : "null").
				setColumn(columnName).
				setReturnedValue(val != null ? val.toString() : "null").
				build(entry));
		return val; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.IColumnTable#getColumnsSerializer()
	 */
	@Override
	public ColumnObject<V> getColumnsSerializer() {
		return table.getColumnsSerializer(); 
	}
}