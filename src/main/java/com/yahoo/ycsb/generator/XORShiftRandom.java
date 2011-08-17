package com.yahoo.ycsb.generator;
import java.util.Random;

public class XORShiftRandom extends Random {
  private long seed = System.nanoTime();

  public XORShiftRandom() {
  }
  public XORShiftRandom(long seed) {
    super(seed);
  }
  public long nextLong() {
    // N.B. Not thread-safe!
    long x = this.seed;
    x ^= (x << 21);
    x ^= (x >>> 35);
    x ^= (x << 4);
    this.seed = x;
    return x;
  }
  protected int next(int nbits) {
    // N.B. Not thread-safe!
    long x = this.seed;
    x ^= (x << 21);
    x ^= (x >>> 35);
    x ^= (x << 4);
    this.seed = x;
    x &= ((1L << nbits) -1);
    return (int) x;
  }

    public void nextBytes(byte[] bytes) { 
        for (int i = 0, len = bytes.length; i < len; ) 
            for (long rnd = nextLong(), 
                     n = Math.min(len - i, Long.SIZE/Byte.SIZE); 
                 n-- > 0; rnd >>= Byte.SIZE) 
                bytes[i++] = (byte)rnd; 
    } 

}
