package com.arondor.common.io;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

public class RandomAccessFile extends java.io.RandomAccessFile implements RandomAccessInterface
{

  public RandomAccessFile(File file, String mode) throws FileNotFoundException
  {
    super(file, mode);
  }

  public RandomAccessFile(String fileName, String mode) throws FileNotFoundException
  {
    super(fileName, mode);
  }

  public int read() throws IOException
  {
    int i = super.read();
    if ( i < 0 )
      i += 256;
    return i;
  }
}
