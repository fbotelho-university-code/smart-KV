/**
Copyright (c) 2007-2013 Alysson Bessani, Eduardo Alchieri, Paulo Sousa, and the authors indicated in the @author tags

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package bftsmart.reconfiguration.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.security.KeyFactory;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.EncodedKeySpec;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;

import org.apache.commons.codec.binary.Base64;

/**
 * Used to load JCA public and private keys from conf/keys/publickey<id> and
 * conf/keys/privatekey<id>
 */
public class RSAKeyLoader {

	private String path;

	private PrivateKey priKey;
	private PublicKey pubKey; 
	
	/** Creates a new instance of RSAKeyLoader */
	public RSAKeyLoader(String configHome) {
		if (configHome.equals("")) {
			path = "config" + System.getProperty("file.separator") + "keys" +
					System.getProperty("file.separator");
		} else {
			path = configHome + System.getProperty("file.separator") + "keys" +
					System.getProperty("file.separator");
		}
		
		try {
			priKey = getPrivateKeyFromString("MIICdwIBADANBgkqhkiG9w0BAQEFAASCAmEwggJdAgEAAoGBALC6hNZtIUOdWOiHB1l+2gDC/egLFQ+dSKI00f2levIRbyRIVEZUnvMkLDEme3dhkaek8bGcLeEAjheQbbqE7v5PGSBpia/kZtqYv3jq5khLG/Ro1fdhto5//8wLoXy1KEV8KelaWPrwVi6Uzl0LY444dpQl+h9CGZd+xoRZTbnXAgMBAAECgYAJaUVdrd4RnbV4XIh1qZ2uYLPowX5ToIqXqLxuB3vunCMRCZEDVcpJJGn+DBCTIO0CwnPkg26mBsOKWbSeNCoN5gOi5yd6Poe0D40ZmvHP1hMCQ9LYhwjLB3Aa+Cl5gYL074Qe/eJFqJaYjZApkeJUAy1HkXhM5OBW9grrXxg6YQJBAPTIni5fG5f2SYutkR2pUydZP4haKVabRkZr8nSHuClGDE2HzbNQjb17z5rRVxJCKMLb2HiPg7ZsUgGK/J1ri78CQQC405h45rL1mCIyZQCXcK/nQZTVi8UaaelKN/ubLQKtTGenJao/zoL+m39i+gGRkHWiG6HNaGFdOkRJmeeH+rfpAkEAn0fwDjKbDP4ZC0fM1uU4k7EyczJgFdgCGY7ifMtXnZvUI5sL0fPH15W6GH7BzsK4LVvK92BDj6/aiOB80p6JlwJASjL4NSE4mwv2PpD5ydI9a/OSEqDIAjCerWMIKWXKe1P/EMU4MeFwCVLXsx523r9F2kyJinLrE4g+veWBY7+tcQJBAKCTm3tbbwLJnnPN46mAgrYb5+LFOmHyNtEDgjxEVrzpQaCChZici2YGY1jTBjb/De4jii8RXllAtUhBEsqyXDA=");
			pubKey = getPublicKeyFromString("MIGfMA0GCSqGSIb3DQEBAQUAA4GNADCBiQKBgQCwuoTWbSFDnVjohwdZftoAwv3oCxUPnUiiNNH9pXryEW8kSFRGVJ7zJCwxJnt3YZGnpPGxnC3hAI4XkG26hO7+TxkgaYmv5GbamL946uZISxv0aNX3YbaOf//MC6F8tShFfCnpWlj68FYulM5dC2OOOHaUJfofQhmXfsaEWU251wIDAQAB");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	/**
	 * Loads the public key of some processes from configuration files
	 *
	 * @return the PublicKey loaded from config/keys/publickey<id>
	 * @throws Exception problems reading or parsing the key
	 */
	public PublicKey loadPublicKey() throws Exception {
		return pubKey; 
	}

	/**
	 * Loads the private key of this process
	 *
	 * @return the PrivateKey loaded from config/keys/publickey<conf.getProcessId()>
	 * @throws Exception problems reading or parsing the key
	 */
	public PrivateKey loadPrivateKey() throws Exception {
		return this.priKey; 
	}

	//utility methods for going from string to public/private key
	private PrivateKey getPrivateKeyFromString(String key) throws Exception {
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		EncodedKeySpec privateKeySpec = new PKCS8EncodedKeySpec(Base64.decodeBase64(key));
		PrivateKey privateKey = keyFactory.generatePrivate(privateKeySpec);
		return privateKey;
	}
	
	private PublicKey getPublicKeyFromString(String key) throws Exception {
		KeyFactory keyFactory = KeyFactory.getInstance("RSA");
		EncodedKeySpec publicKeySpec = new X509EncodedKeySpec(Base64.decodeBase64(key));
		PublicKey publicKey = keyFactory.generatePublic(publicKeySpec);
		return publicKey;
	}
}
