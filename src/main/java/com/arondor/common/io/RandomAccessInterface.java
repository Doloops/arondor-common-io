package com.arondor.common.io;

import java.io.IOException;

public interface RandomAccessInterface
{

    int readInt() throws IOException;

    int readUnsignedShort() throws IOException;

    void seek(long offset) throws IOException;

    long length() throws IOException;

    long getFilePointer() throws IOException;

    int read(byte[] bytes, int offset, int count) throws IOException;

    void close() throws IOException;

    public void writeByte(int i) throws IOException;

    void writeShort(int value) throws IOException;

    void writeInt(int i) throws IOException;

    void write(byte[] bytes) throws IOException;

    int read() throws IOException;

    void write(byte[] b, int offset, int length) throws IOException;

}
