package com.awstest;

import java.io.InputStream;

/**
 * Simple input stream using generated printable characters
 */
public class GeneratedInputStream extends InputStream {
    public static final long MAX = 9999999999L;
    private final byte[] size;
    private final long bytes;
    private long sent;

    /**
     *
     * @param bytes number of bytes in the stream (after the length header)
     */
    public GeneratedInputStream(long bytes){
        bytes = bytes < 0 ? 0 : Math.min(bytes, MAX);
        size = String.format("%010d", bytes).getBytes();
        this.bytes = bytes + 10; // sending 10 bytes for the length
    }

    /**
     * @return next char in the stream
     * First 10 bytes returned are the stream length
     *
     */
    @Override
    public int read() {
        if (sent < 10)
            return size[(int) sent++];

        return (int)(sent >= bytes ? -1 : sent++ % 95 + 32);
    }

    @Override
    public int read(byte[] b) {
        int count;

        for(count = 0; count < b.length; count++) {
            b[count] = (byte)read();
        }
        return count == 0 ? -1 : count;
    }
}
