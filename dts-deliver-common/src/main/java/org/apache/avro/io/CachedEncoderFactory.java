
package org.apache.avro.io;

import java.io.OutputStream;

public class CachedEncoderFactory extends EncoderFactory {

    private static final CachedEncoderFactory DEFAULT_FACTORY = new CachedEncoderFactory();

    public static CachedEncoderFactory getDefault() {
        return DEFAULT_FACTORY;
    }

    public CachedBufferedBinaryEncoder binaryEncoder(OutputStream out, CachedBufferedBinaryEncoder reuse, boolean usePool) {
        if (null == reuse) {
            return new CachedBufferedBinaryEncoder(out, super.binaryBufferSize * 2, usePool);
        } else {
            return (CachedBufferedBinaryEncoder) reuse.configure(out, this.binaryBufferSize * 2);
        }
    }
}
