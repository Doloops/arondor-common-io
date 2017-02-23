package com.arondor.common.io;

import java.io.IOException;

import org.apache.log4j.Logger;

public class RandomAccessBuffer implements RandomAccessInterface
{
    private static final Logger log = Logger.getLogger(RandomAccessBuffer.class);

    private boolean verbose = false;

    private long offset = 0;

    private long length = 0;

    private byte[] bytes;

    public RandomAccessBuffer(byte[] bytes)
    {
        this.bytes = bytes;
        this.length = bytes.length;
    }

    public RandomAccessBuffer()
    {
        this.bytes = null;
        this.length = 0;
    }

    public void close() throws IOException
    {

    }

    public long getFilePointer() throws IOException
    {
        return offset;
    }

    public long length() throws IOException
    {
        return length;
    }

    public int read(byte[] bytes, int boffset, int bcount) throws IOException
    {
        int pos = bcount;
        // for ( pos = 0; (offset + pos) < length && ( boffset + pos ) <
        // bytes.length && pos < bcount ; pos ++ )
        // {
        // bytes[boffset + pos] = this.bytes[(int)offset + pos];
        // }
        if (boffset > offset)
            return 0;
        if (offset + pos > length)
            pos = (int) (length - offset);
        try
        {
            System.arraycopy(this.bytes, (int) offset, bytes, boffset, pos);
        }
        catch (ArrayIndexOutOfBoundsException aioobe)
        {
            throw new IOException("offset=" + offset + ", boffset=" + boffset + ", bcount=" + bcount + ", pos=" + pos,
                    aioobe);
        }
        offset += pos;
        return pos;
    }

    protected int doReadByte() throws IOException
    {
        if (offset >= length)
            throw new IOException("Out of bounds : offset=" + offset + ", length=" + length);
        byte c = this.bytes[(int) offset];
        // int i = (c < 0) ? (c+128) : c;
        int i = c;
        if (i < 0)
            i += 256;
        if (i < 0 || i >= 256)
            throw new RuntimeException("Invalid i=" + i);
        if (verbose)
            log.debug("offset=" + offset + ", c=" + c + ", i=" + i + ", 0x=" + Integer.toHexString(i));
        offset++;
        return i;
    }

    public void writeByte(int i) throws IOException
    {
        doWriteByte(i);
    }

    protected void doWriteByte(int i) throws IOException
    {
        if (bytes == null || offset >= bytes.length)
        {
            // throw new IOException("Out of bounds : offset=" + offset +
            // ", length=" + length);
            int toAllocate = (bytes != null && bytes.length > 0) ? (bytes.length * 2) : 256;
            byte[] newBytes = new byte[toAllocate];
            if (bytes != null && bytes.length > 0)
            {
                // for ( int p = 0 ; p < bytes.length ; p++ )
                // newBytes[p] = bytes[p];
                System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
            }
            bytes = newBytes;
        }
        if (i < -128 || i > 255)
            throw new RuntimeException("Invalid value : i=" + i);
        this.bytes[(int) offset] = (byte) i;
        offset++;
        if (length < offset)
            length = offset;
    }

    public int readInt() throws IOException
    {
        int r = (doReadByte() << 24) + (doReadByte() << 16) + (doReadByte() << 8) + doReadByte();
        if (verbose)
            log.debug("Read r=" + r + ", 0x" + Integer.toHexString(r));
        return r;
    }

    public int readUnsignedShort() throws IOException
    {
        int r = (doReadByte() << 8) + doReadByte();
        if (verbose)
            log.debug("Read r=" + r + ", 0x" + Integer.toHexString(r));
        return r;
    }

    public void seek(long offset) throws IOException
    {
        if (offset == this.length)
        {
            log.warn("Seeking at the end of stream ! offset=" + offset + ", length=" + this.length);
            // throw new IOException("seek(" + offset +
            // ") at the end of stream !, length=" + this.length);
        }
        else if (offset < 0 || (this.length > 0 && offset >= this.length))
        {
            throw new IOException("seek(" + offset + ") out of range, length=" + this.length);
        }
        this.offset = offset;
    }

    public void write(byte[] bytes) throws IOException
    {
        for (int i = 0; i < bytes.length; i++)
            doWriteByte(bytes[i]);
    }

    public void writeInt(int value) throws IOException
    {
        doWriteByte(value >> 24);
        doWriteByte((value >> 16) & 0xff);
        doWriteByte((value >> 8) & 0xff);
        doWriteByte(value & 0xff);
    }

    public void writeShort(int value) throws IOException
    {
        if (value > (1 << 16))
        {
            throw new RuntimeException("Invalid value=" + value);
        }
        doWriteByte(value >> 8 % 256);
        doWriteByte(value % 256);
    }

    public byte[] getBytes()
    {
        // byte b[] = new byte[(int) this.length];
        // for ( int p = 0 ; p < this.length ; p++ )
        // b[p] = this.bytes[p];
        // return b;
        if (this.bytes.length < this.length)
            throw new RuntimeException("Invalid : bytes=" + this.bytes.length + ", length=" + this.length);
        return java.util.Arrays.copyOfRange(this.bytes, 0, (int) this.length);
    }

    public int read() throws IOException
    {
        return doReadByte();
    }

    public void write(byte[] b, int offset, int length) throws IOException
    {
        for (int p = 0; p < length; p++)
            doWriteByte(bytes[offset + p]);
    }

}
