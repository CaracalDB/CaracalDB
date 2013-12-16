package se.sics.caracaldb.api.data

import se.sics.caracaldb.Key
import java.security.MessageDigest

object KeyUtil {
	
	val digest = MessageDigest.getInstance("MD5");

	def schemaToKey(schema: String, key: String): Key = {
		val schemaHash = digest.digest(schema.getBytes("UTF-8"));
		if (key == null) {
			return new Key(schemaHash);
		}
		val k = Key.fromHex(correctFormat(key));		
		return k.prepend(schemaHash).get();
	}
	
	def correctFormat(key: String): String = {
		val str = new StringBuilder();
		var count = 0;
		for (c <- key) {
			if (count == 2) {
				count = 0;
				if (!c.isSpaceChar) {
					str += ' ';
					count = 1;
				}
				str += c;
			} else {
				str += c;
				count += 1;
			}
		}
		return str.result();
	}
}