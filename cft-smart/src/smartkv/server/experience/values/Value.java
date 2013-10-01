/**
 * 
 */
package smartkv.server.experience.values;

import java.io.Serializable;


/**
 * @author fabiim
 *
 */
public interface Value extends Serializable {
	public static  enum SingletonValues implements Value{
		EMPTY(null),
		TRUE(new byte[1]),
		FALSE(null); 
		
		private final  byte[] byteRepresentation; 
		private SingletonValues(final byte[] val){
			this.byteRepresentation = val; 
		}

		/* (non-Javadoc)
		 * @see smartkv.server.experience.values.Value#asByteArray()
		 */
		@Override
		public byte[] asByteArray() {
			return this.byteRepresentation; 
		}

		/* (non-Javadoc)
		 * @see smartkv.server.experience.values.Value#arrangeDataDeterministically()
		 */
		@Override
		public void arrangeDataDeterministically() {
			// TODO Auto-generated method stub
			
		}
		
	}
	
	
	//XXX too lazy to delete this. Use singletonValues directly... 
	public static final Value TRUE = SingletonValues.TRUE;
	public static final Value FALSE = SingletonValues.FALSE ;
	
	public byte[] asByteArray(); 
	/**
	 * Arrange the value such that asByteArray will be equal in every replica. 
	 * This is important. Imagine that Value maintains non-deterministic data (such as HashMap). 
	 * Then, {@link #asByteArray()} can return different values in the replicas. We can not allow this 
	 * unless the client software would be able to accurately determine that the values from the replicas is conceptually 
	 * the same, despite the differences in the reply. 
	 */
	public void arrangeDataDeterministically() ;
	
	@Override
	public boolean equals(Object v); 
}
