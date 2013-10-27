/**
 * 
 */
package smartkv.client.tables;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;

import smartkv.client.util.Serializer;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * @author fabiim
 *
 */
public class AnnotatedColumnObject<T> implements ColumnObject<T>{
	//TODO: set loggers up and running 
	//private static final Logger log = Logger.getLoger("com.bonafide"); 
	public static interface Constructor<T>{
		T newInstance(); 
	}
	
	public static  Map<Class<?>, AnnotatedColumnObject<?>> instances = Maps.newHashMap(); 

	//FIXME 
	@SuppressWarnings("unchecked")
	public synchronized static <T> AnnotatedColumnObject<T> newAnnotatedColumnObject(Class<T> type){
		return AnnotatedColumnObject.newAnnotatedColumnObject(type, new HashMap<String,Serializer>()); 
	}
	
	public synchronized static <T> AnnotatedColumnObject<T> newAnnotatedColumnObject(Class<T> type, Map<String, Serializer> serializers){
		if (!instances.containsKey(type)){
			instances.put(type, new AnnotatedColumnObject<T>(type, serializers)); 
		}
		return (AnnotatedColumnObject<T>) instances.get(type);
	}
	
	private final Map<String,Method> getters;
	private final Map<String,Method> setters;
	private final Map<String,Serializer>  serializers; 
	private final Class<T> clazz;
	private AnnotatedColumnObject.Constructor<T>  constructor; 
	
	/**
	 * FIXME: documentation. 
	 * @throws IllegalArgumentException if type does not implements an appropriate constructor.  
	 */
	private AnnotatedColumnObject(Class<T> type, Map<String,Serializer> serializers){
		clazz = type; 
		try {
			type.newInstance();
		} catch (Exception e){
			//XXX  be nicer, get info from exception e maybe... 
			throw new IllegalArgumentException("Class : " + type.getName() + " does not implements a valid constructor. Reason: " + e.getMessage()); 
		}
		
		///XXX could i have used final in paremeter? 
		this.constructor =new Constructor<T> (){
			@Override
			public T newInstance() {
				//TODO :cleanup
				try {
					return clazz.newInstance();
				} catch (InstantiationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IllegalAccessException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				return null; 
			}
		};
		Map<String,Method> setters = Maps.newHashMap(); 
		Map<String,Method> getters = Maps.newHashMap();
		_construct(type, getters, setters, serializers);
		this.setters = ImmutableMap.copyOf(setters);
		this.getters =ImmutableMap.copyOf(getters);
		this.serializers = ImmutableMap.copyOf(serializers); 
		
	}
	
	private AnnotatedColumnObject(Class<T> type, Constructor<T> constructor, Map<String,Serializer> serializers){
		this.clazz = type; 
		this.constructor = constructor; 
		Map<String,Method> setters = Maps.newHashMap(); 
		Map<String,Method> getters = Maps.newHashMap(); 
		_construct(type, getters, setters, serializers);
		this.setters = ImmutableMap.copyOf(setters);
		this.getters =ImmutableMap.copyOf(getters);
		this.serializers = ImmutableMap.copyOf(serializers); 
	}
	
	
	//XXX is this ok (method shared by constructors) ? 
	@SuppressWarnings("unchecked")
	private final void _construct(Class<T> type, Map<String,Method> getters, Map<String,Method> setters, Map<String, Serializer> serializers){
		
		for (Method m : type.getDeclaredMethods()) {
			if (m.isAnnotationPresent(Column.class)) {
				String getter = m.getAnnotation(Column.class).getter();
				//FIXME
				if (getter.equals("DEFAULT")){
					getter = m.getName(); 
				}
				getters.put(getter, m); 
			    String setter = m.getAnnotation(Column.class).setter(); 
			    if (setter.equals("DEFAULT")){
			    	setter = "set" +  m.getName().substring(3);
			    }
				Method setterMethod = null;;
				try {
					
					setterMethod = type.getMethod( setter, m.getReturnType());
					if (setterMethod == null){
			    		//FIXME
						throw new IllegalArgumentException("Method: " + m.getName() + " is qualified by  Column with default setter, but there is no method: " + setter ); 
			    	}
				} catch (Exception e ) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					throw new IllegalArgumentException("Method: " + m.getName() + " is qualified by  Column with default setter, but there is no method: " + setter ); 
				}
				setters.put( getter, setterMethod);
				if (!serializers.containsKey(getter)){
					serializers.put(getter, m.getAnnotation(Column.class).serializer().serial);
				}
			}
		}
	}
	//TODO: T implements serializable?
	
	/* 
	 * @see bonafide.datastore.tables.ColumnObject#toColumns(java.lang.Object)
	 */
	@SuppressWarnings("unchecked")
	@Override
	public TreeMap<String, byte[]> toColumns(T obj){
		TreeMap<String, byte[]> values= Maps.newTreeMap();  
		
		for (Entry<String, Method> field : getters.entrySet()){
			try {
				Object v = field.getValue().invoke(obj);
				//No need to serialize null references...
				if (v != null){
					byte[] vv = this. serializers.get(field.getKey()).serialize(field.getValue().invoke(obj));
					if (vv != null){
						values.put(field.getKey(), vv); 
					}
					else{
						try{
							throw new Exception("Null value: "+ field.getKey());
						}catch (Exception e){
							e.printStackTrace(); 
						}
					}
				}

			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return values; 
	}
	
	//FIXME: try running methods once at constructor... ignore exceptions later...
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnObject#fromColumns(java.util.Map)
	 */
	@Override
	public T fromColumns(Map<String, byte[]> fields) {
		T object = constructor.newInstance();
		for (Entry<String, byte[]> en: fields.entrySet()){
			Method m = setters.get(en.getKey());
			if (m!= null){
			try {
				Object o = serializers.get(en.getKey()).deserialize(en.getValue());
				m.invoke(object, o);
				
			} catch (IllegalArgumentException e) {
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}}
			else{
				//System.out.println("Data store object has unknown columns: " + en.getKey());
				//log.warn("Data store object has unknown columns: ");				
			}
		}
		return object;
	}
	
	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnObject#serializeColumn(java.lang.String, java.lang.Object)
	 */
	@Override
	public byte[] serializeColumn(String columnName, Object val) {
		return serializers.get(columnName).serialize(val); 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnObject#deserializeColumn(java.lang.String, byte[])
	 */
	@Override
	public Object deserializeColumn(String columnName, byte[] val) {
		return  val != null ?  serializers.get(columnName).deserialize(val): null; 
	}

	/* (non-Javadoc)
	 * @see smartkv.client.tables.ColumnObject#getColumn(java.lang.Object, java.lang.String)
	 */
	@Override
	public <C> C getColumn(T t, String columnName) {
		try {
			return (C) getters.get(columnName).invoke(t);
		} catch (IllegalArgumentException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		} catch (InvocationTargetException e) {
			e.printStackTrace();
		}
		return null; 
	}

}
