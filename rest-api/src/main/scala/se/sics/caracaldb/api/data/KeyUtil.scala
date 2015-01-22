package se.sics.caracaldb.api.data

import se.sics.caracaldb.Key
import java.security.MessageDigest

object KeyUtil {

	def stringToKey(key: String): Key = {
        if (key == null) {
            return Key.NULL_KEY;
        }
        return Key.fromHex(correctFormat(key));
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