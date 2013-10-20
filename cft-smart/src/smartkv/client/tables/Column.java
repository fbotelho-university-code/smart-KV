/**
 * 
 */
package smartkv.client.tables;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import smartkv.client.util.Serializer.SerialNum;

/**
 * @author fabiim
 *
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Column {
	
	
	//FIXME: change default name. 
	String getter() default "DEFAULT";

	/**
	 * @return
	 */
	String setter() default "DEFAULT";
	SerialNum serializer() default SerialNum.UNSAFE;  
}


