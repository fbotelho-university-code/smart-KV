package smartkv.client.workloads;

import java.util.Set;

import smartkv.client.workloads.RequestLogWithDataInformation.Builder;


public class  RequestLogWithDataInformation extends RequestLogEntry{
	/**
	 * 
	 */
	public static class Builder {
		private String table ="-"; 
		private String key ="-"; 
		private String value ="-"; 
		private String existentValue ="-";
		private String returnedValue = "-";
		private String column = "-";
		private Set<String> columns; 
		

		public Set<String> getColumns(){
			return columns; 
		}
		public Builder setTable(String table) {
			this.table = table;
			return this; 
		}
		public Builder setKey(String key) {
			this.key = key;
			return this; 
		}
		public Builder setValue(String value) {
			this.value = value;
			return this;
			
		}
		public Builder setExistentValue(String existentValue) {
			this.existentValue = existentValue;
			return this; 
		}
		public Builder setReturnedValue(String returnedValue) {
			this.returnedValue = returnedValue;
			return this; 
		}
		public Builder setColumn(String columnName) {
			this.column = columnName; 
			return this; 
		}
		public String getColumn() {
			return column;
		}
		public String getTable() {
			return table;
		}
		public String getKey() {
			return key;
		}
		public String getValue() {
			return value;
		}
		public String getExistentValue() {
			return existentValue;
		}
		public String getReturnedValue() {
			return returnedValue;
		}
		
		public RequestLogWithDataInformation build(){
			return new RequestLogWithDataInformation(this); 
		}
		public RequestLogWithDataInformation build(RequestLogEntry request){
			return new RequestLogWithDataInformation(this, request); 
		}
		/**
		 * @param columnName
		 * @return
		 */
		/**
		 * @param columns
		 * @return
		 */
		public Builder setColumns(Set<String> columns) {
			this.columns = columns; 
			return this; 
		}
		
	}
	
	private static final long serialVersionUID = 1L;
	private String table; 
	private String key; 
	private String value; 
	private String existentValue; 
	private String returnValue; 
	private String column; 
	private Set<String> columns; 
	
	///XXX - mete nojo isto. 
	public RequestLogWithDataInformation(Builder b){
		this(b, null);
		initializeFields(); 
	}
	
	public RequestLogWithDataInformation(Builder b, RequestLogEntry req){
		super(req);
		this.table = b.getTable(); 
		this.key = b.getKey(); 
		this.value =b.getValue(); 
		this.existentValue = b.getExistentValue();
		this.returnValue = b.getReturnedValue(); 
		this.column =b.getColumn(); 
	}
	
	
	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getKey() {
		return key;
	}

	public void setKey(String key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

	public String getExistentValue() {
		return existentValue;
	}

	public void setExistentValue(String existentValue) {
		this.existentValue = existentValue;
	}
	
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = super.hashCode();
		result = prime * result + ((column == null) ? 0 : column.hashCode());
		result = prime * result
				+ ((existentValue == null) ? 0 : existentValue.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result
				+ ((returnValue == null) ? 0 : returnValue.hashCode());
		result = prime * result + ((table == null) ? 0 : table.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		RequestLogWithDataInformation other = (RequestLogWithDataInformation) obj;
		if (column == null) {
			if (other.column != null)
				return false;
		} else if (!column.equals(other.column))
			return false;
		if (existentValue == null) {
			if (other.existentValue != null)
				return false;
		} else if (!existentValue.equals(other.existentValue))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (returnValue == null) {
			if (other.returnValue != null)
				return false;
		} else if (!returnValue.equals(other.returnValue))
			return false;
		if (table == null) {
			if (other.table != null)
				return false;
		} else if (!table.equals(other.table))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}
	
	
	@Override
	public String toString() {
		return  super.toString() + " - RequestLogWithDataInformation [table=" + table + ", key=" + key
				+ ", value=" + value + ", existentValue=" + existentValue
				+ ", returnValue=" + returnValue + ", column=" + column + "]";
	}

	/**
	 * @return
	 */
	public String getReturnedValue() {
		return  returnValue; 
	}
	
}
