package org.apache.avro.io;

import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;

public class CachedBufferedBinaryEncoder extends BufferedBinaryEncoder {

    private HashMap<String, byte[]> utf8Charsets;
    private boolean useCache;

    public CachedBufferedBinaryEncoder(OutputStream out, int bufferSize, boolean useCache) {
        super(out, bufferSize);
        this.utf8Charsets = new HashMap<>();
        this.useCache = useCache;
    }

    public void writeString(String string, boolean usePool) throws IOException {
        if (this.useCache) {
            if (!usePool || string.length() > 32 || 0 == string.length()) {
                super.writeString(string);
                return;
            }
            byte[] bytes = utf8Charsets.get(string);
            if (null == bytes) {
                bytes = string.getBytes("UTF-8");
                utf8Charsets.put(string, bytes);
            }
            writeInt(bytes.length);
            writeFixed(bytes, 0, bytes.length);
        } else {
            super.writeString(string);
        }
    }

    public void writeAsciiString(String string) throws IOException {
        if (0 == string.length()) {
            super.writeString(string);
        } else {
            byte[] bytes = new byte[string.length()];
            for (int i = 0; i < bytes.length; ++i) {
                bytes[i] = (byte) string.charAt(i);
            }
            writeInt(bytes.length);
            writeFixed(bytes, 0, bytes.length);
        }
    }
}
