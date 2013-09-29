/**
 * 
 */
package smartkv.datastore.tables;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.Map.Entry;

import org.slf4j.Logger;

import smartkv.datastore.util.Serializer;
import smartkv.datastore.util.UnsafeJavaSerializer;

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
		if (!instances.containsKey(type)){
			instances.put(type, new AnnotatedColumnObject<T>(type)); 
		}
		return (AnnotatedColumnObject<T>) instances.get(type);
	}
	
	
	private final Map<String,Method> getters;
	private final Map<String,Method> setters;
	private final Class<T> clazz;
	private final Serializer<Object> serializer = UnsafeJavaSerializer.getInstance(); 
	private AnnotatedColumnObject.Constructor<T>  constructor; 
	
	/**
	 * FIXME: documentation. 
	 * @throws IllegalArgumentException if type does not implements an appropriate constructor.  
	 */
	private AnnotatedColumnObject(Class<T> type){
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
		_construct(type, getters, setters);
		this.setters = ImmutableMap.copyOf(setters);
		this.getters =ImmutableMap.copyOf(getters); 
	}
	
	private AnnotatedColumnObject(Class<T> type, Constructor<T> constructor){
		this.clazz = type; 
		this.constructor = constructor; 
		Map<String,Method> setters = Maps.newHashMap(); 
		Map<String,Method> getters = Maps.newHashMap(); 
		_construct(type, getters, setters);
		this.setters = ImmutableMap.copyOf(setters);
		this.getters =ImmutableMap.copyOf(getters); 
	}
	
	
	//XXX is this ok (method shared by constructors) ? 
	private final void _construct(Class<T> type, Map<String,Method> getters, Map<String,Method> setters){
		
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
			}
		}
	}
	//TODO: T implements serializable?
	
	/* 
	 * @see bonafide.datastore.tables.ColumnObject#toColumns(java.lang.Object)
	 */
	@Override
	public Map<String, byte[]> toColumns(T obj){
		Map<String, byte[]> values= Maps.newHashMap();  
		
		for (Entry<String, Method> field : getters.entrySet()){
			try {
				values.put(field.getKey(), serializer.serialize(field.getValue().invoke(obj)));
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
				
				m.invoke(object, serializer.deserialize(en.getValue()));
				
			} catch (IllegalArgumentException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IllegalAccessException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (InvocationTargetException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}}
			else{
				System.out.println("Data store object has unknown columns: " + en.getKey());
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
		return serializer.serialize(val); 
	}

	/* (non-Javadoc)
	 * @see bonafide.datastore.tables.ColumnObject#deserializeColumn(java.lang.String, byte[])
	 */
	@Override
	public Object deserializeColumn(String columnName, byte[] val) {
		return  val != null ?  serializer.deserialize(val): null; 
	}

}
