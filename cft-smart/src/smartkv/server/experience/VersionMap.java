/**
 * 
 */
package smartkv.server.experience;

import java.util.HashMap;
import java.util.Map;

import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;
import smartkv.server.experience.values.VersionedValue;

/**
 * @author fabiim
 *
 */
public class VersionMap extends NonNullValueMap{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	@Override
	public Value put(Key key, Value value) {
		Value bucket = super.get(key);
		Value oldValue =Value.SingletonValues.EMPTY;
		VersionedValue valueBucket = bucket != Value.SingletonValues.EMPTY ? (VersionedValue) bucket : null;
		if (valueBucket!= null ){
			//We change the value even if it is equal to the previous. This is nice to let the client known the new timestamp according to the result of the operation.
			//If we were not using a cache, then it would be different.   
			oldValue = new VersionedValue(valueBucket.getValue(), valueBucket.getVersion());
			valueBucket.setValue(value);
		}
		else{
			valueBucket = new VersionedValue(value);
			super.put(key, valueBucket);
		}
		
		return oldValue; 
	}

	@Override
	public void putAll(Map<? extends Key, ? extends Value> m) {
		throw new UnsupportedOperationException("Bypassing timestamp mechanism");
	}

}
