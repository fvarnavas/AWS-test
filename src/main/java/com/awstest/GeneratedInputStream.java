package com.awstest;

import java.io.IOException;
import java.io.InputStream;

public class GeneratedInputStream extends InputStream {
    static final long MAX = 9999999999L;
    long bytes;
    long sent;
    byte[] size;

    /** generate up to 9999999999 printable bytes in a stream
     *
     * @param bytes number of bytes in the stream
     */
    GeneratedInputStream(long bytes){
        this.bytes = bytes < 0 ? 0 : Math.min(bytes, MAX);
        size = String.format("%010d",this.bytes).getBytes();
        this.bytes += 10; // sending 10 bytes for the length
    }

    /**
     * @return next char in the stream
     * First 10 bytes returned are the stream length
     *
     * @throws IOException
     */
    @Override
    public int read() throws IOException {
        if (sent < 10)
            return size[(int) sent++];

        return (int)(sent > bytes ? -1 : sent++ % 95 + 32);
    }
}
