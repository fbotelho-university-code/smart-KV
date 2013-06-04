package mapserver;

import java.io.Serializable;


public enum RequestType implements Serializable{
	CREATE_TABLE(SuperType.WRITE, "Creates a Table in the Datastore"),  
	REMOVE_TABLE(SuperType.WRITE, "Removes a Table from the Datastore"),
	CONTAINS_TABLE(SuperType.READ, "Boolean method to check if a table exists"),
	CLEAR_DATASTORE(SuperType.WRITE, "Clear all the Datastore"), 
	CLEAR_TABLE(SuperType.WRITE, "Clear a table"), 
	CONTAINS_KEY_IN_TABLE(SuperType.READ, "Check if a table contains a key"),
	GET_TABLE(SuperType.READ, "Get all the contents from a table"), 
	GET_VALUE_IN_TABLE(SuperType.READ, "Get a specific value from the table"), 
	IS_DATASTORE_EMPTY(SuperType.READ, "Check if all the datastore is empyt (i.e., no tables)"), 
	IS_TABLE_EMPTY(SuperType.READ, "Check if a table is empyt (i.e., no values)"), 
	PUT_VALUE_IN_TABLE(SuperType.WRITE, "Put a value in a table"), 
	PUT_VALUES_IN_TABLE(SuperType.WRITE, "Put several values in a table"), 
	REMOVE_VALUE_FROM_TABLE(SuperType.WRITE, "Remove a value from a table"), 
	SIZE_OF_TABLE(SuperType.READ, "Get size of tables (in entries)"), 
	CREATE_TABLE_MAX_SIZE(SuperType.READ, "Create a circular buffer table (i.e., will remove oldest entry after hiting the threshold specified"), 
	ATOMIC_REPLACE_VALUE_IN_TABLE (SuperType.WRITE, "Atomically replace a value in a table if the provided expectedValue is correct"), 
	ATOMIC_REMOVE_IF_VALUE(SuperType.WRITE, "Atomically remove a value in a table if the provided expectedValue is found"), 
	ATOMIC_PUT_IF_ABSENT(SuperType.WRITE, "Atomically set a value in a table if the key is already present in the table"), 
	GET_AND_INCREMENT(SuperType.WRITE, "Get and Increment a value");
	
	public enum SuperType{
		WRITE,
		READ;
		
		@Override
		public String toString(){
			return name(); 
		}
	}
	
	public  final SuperType type;
	public final String description; 
	
	RequestType(SuperType t, String description){
		this.type = t;
		this.description = description; 
	}
	
	public boolean isWrite(){
		return type == SuperType.WRITE; 
	}
	
	public boolean isRead(){
		return type == SuperType.READ; 
	}
	
	@Override
	public String toString(){
		return super.toString() + " - " + this.description;  
	}
	
}
