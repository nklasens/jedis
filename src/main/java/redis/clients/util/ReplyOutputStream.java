package redis.clients.util;

import java.io.*;

public class ReplyOutputStream extends FilterOutputStream {

  private byte separator;
  private boolean nextReply = false;

  public ReplyOutputStream(OutputStream out, byte separator) {
    super(out);
    this.separator = separator;
  }
  
  public void finishReply() {
    this.nextReply = true;
  }

  protected void writeNext() throws IOException {
    if (nextReply) {
      super.write(separator);
      nextReply = false;
    }
  }

  @Override
  public void write(int b) throws IOException {
    writeNext();
    super.write(b);
  }

  @Override
  public void write(byte[] b) throws IOException {
    writeNext();
    super.write(b);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    writeNext();
    super.write(b, off, len);
  }
  
}
