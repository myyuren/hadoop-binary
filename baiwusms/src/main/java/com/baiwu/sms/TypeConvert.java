package com.baiwu.sms;

public class TypeConvert
{
  public static byte[] intToByteArray(int i)
  {
    byte[] result = new byte[4];

    result[0] = ((byte)(i >> 24 & 0xFF));
    result[1] = ((byte)(i >> 16 & 0xFF));
    result[2] = ((byte)(i >> 8 & 0xFF));
    result[3] = ((byte)(i & 0xFF));
    return result;
  }

  public static int byteArrayToInt(byte[] bytes)
  {
    int value = 0;

    for (int i = 0; i < 4; i++) {
      int shift = (3 - i) * 8;
      value += ((bytes[i] & 0xFF) << shift);
    }
    return value;
  }

  public static byte[] longToByte(long number) {
    long temp = number;
    byte[] b = new byte[8];
    for (int i = 0; i < b.length; i++) {
      b[i] = new Long(temp & 0xFF).byteValue();
      temp >>= 8;
    }
    return b;
  }

  public static long byteToLong(byte[] b)
  {
    long s = 0L;
    long s0 = b[0] & 0xFF;
    long s1 = b[1] & 0xFF;
    long s2 = b[2] & 0xFF;
    long s3 = b[3] & 0xFF;
    long s4 = b[4] & 0xFF;
    long s5 = b[5] & 0xFF;
    long s6 = b[6] & 0xFF;
    long s7 = b[7] & 0xFF;

    s1 <<= 8;
    s2 <<= 16;
    s3 <<= 24;
    s4 <<= 32;
    s5 <<= 40;
    s6 <<= 48;
    s7 <<= 56;
    s = s0 | s1 | s2 | s3 | s4 | s5 | s6 | s7;
    return s;
  }

  public static byte[] shortToByte(short number) {
    int temp = number;
    byte[] b = new byte[2];
    for (int i = 0; i < b.length; i++) {
      b[i] = new Integer(temp & 0xFF).byteValue();
      temp >>= 8;
    }
    return b;
  }

  public static short byteToShort(byte[] b) {
    short s = 0;
    short s0 = (short)(b[0] & 0xFF);
    short s1 = (short)(b[1] & 0xFF);
    s1 = (short)(s1 << 8);
    s = (short)(s0 | s1);
    return s;
  }

  public static byte[] doubleToByteArray(double x, int index)
  {
    byte[] b = new byte[8];
    long l = Double.doubleToLongBits(x);
    for (int i = 0; i < 4; i++) {
      b[(index + i)] = new Long(l).byteValue();
      l >>= 8;
    }
    return b;
  }

  public static double byteArrayToDouble(byte[] b, int index)
  {
    long l = b[0];
    l &= 255L;
    l |= b[1] << 8;
    l &= 65535L;
    l |= b[2] << 16;
    l &= 16777215L;
    l |= b[3] << 24;
    l &= 4294967295L;
    l |= b[4] << 32;
    l &= 1099511627775L;
    l |= b[5] << 40;
    l &= 281474976710655L;
    l |= b[6] << 48;
    l &= 72057594037927935L;
    l |= b[7] << 56;
    return Double.longBitsToDouble(l);
  }

  public static byte[] byteMerger(byte[] byte_1, byte[] byte_2) {
    byte[] byte_3 = new byte[byte_1.length + byte_2.length];
    System.arraycopy(byte_1, 0, byte_3, 0, byte_1.length);
    System.arraycopy(byte_2, 0, byte_3, byte_1.length, byte_2.length);
    return byte_3;
  }
}