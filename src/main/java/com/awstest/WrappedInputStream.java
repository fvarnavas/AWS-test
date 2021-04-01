package com.awstest;

import java.io.IOException;
import java.io.InputStream;

public class WrappedInputStream extends InputStream {
    private final InputStream is;

    public WrappedInputStream(InputStream is){
        this.is = is;
    }

    @Override
    public int read() throws IOException {
        return is.read();
    }

    @Override
    public int read(byte[] b) throws IOException {
        return is.read(b);
    }
}
