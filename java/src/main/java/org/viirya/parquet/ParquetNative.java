package org.viirya.parquet;

class ParquetNative {
  public static native String loadParquetFile(String filePath);

  static {
    // This actually loads the shared object that we'll be creating.
    // The actual location of the .so or .dll may differ based on your
    // platform.
    System.loadLibrary("parquetlib");
  }
}