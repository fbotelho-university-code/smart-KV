/**
 * 
 */
package smartkv.client.tables;

import java.io.Serializable;



public  class ValueObject implements Serializable{
	public int id; 
	public String cenas; 

	public ValueObject(){
		
	}
	public ValueObject(int id){
		this.id = id; 
		this.cenas = String.valueOf(id);
	}
	
	
	@Column
	public int getId() {
		return id;
	}




	public void setId(int id) {
		this.id = id;
	}



	@Column
	public String getCenas() {
		return cenas;
	}




	public void setCenas(String cenas) {
		this.cenas = cenas;
	}




	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + id;
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
		ValueObject other = (ValueObject) obj;
		if (id != other.id)
			return false;
		return true;
	}
	
}