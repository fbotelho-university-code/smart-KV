/**
 * 
 */
package smartkv.server.experience;

import java.util.HashMap;

import smartkv.server.experience.values.Key;
import smartkv.server.experience.values.Value;

/**
 * @author fabiim
 *
 */
public class NonNullValueMap extends HashMap<Key,Value>{

	@Override
	public Value get(Object arg0) {
		Value v =  super.get(arg0);
		return valueOrEmpty(v);
	}

	/**
	 * @param v
	 * @return
	 */
	private Value valueOrEmpty(Value v) {
		return v != null ? v : Value.SingletonValues.EMPTY; 
	}

	@Override
	public Value put(Key arg0, Value arg1) {
		return valueOrEmpty(super.put(arg0, arg1));
	}

	@Override
	public Value remove(Object arg0) {
		return valueOrEmpty(super.remove(arg0));
	}
	
}
