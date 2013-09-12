package mapserver;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Comparator;

import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.primitives.UnsignedBytes;


public  class ByteArrayWrapper implements Serializable{
		/**
		 * 
		 */
		private static final long serialVersionUID = 1L;

		public final byte[] value;

		//TODO - change this for something that can be serializable?
		public static final HashFunction hf = Hashing.murmur3_32();
		
		
		public ByteArrayWrapper(byte[] v){
			value = v; 
		}


		public final static Comparator<ByteArrayWrapper> COMPARE = new Comparator<ByteArrayWrapper>(){

			@Override
			public int compare(ByteArrayWrapper o1, ByteArrayWrapper o2) {
				//FIXME
				return UnsignedBytes.lexicographicalComparator().compare(o1.value, o2.value); 
			}
		};
		
		@Override
		public final boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			ByteArrayWrapper other = (ByteArrayWrapper) obj;
			if (!Arrays.equals(value, other.value))
				return false;
			return true;
		}
		
		@Override
		public  final
		int hashCode(){
			return hf.hashBytes(value).asInt(); 
			
		}

	}