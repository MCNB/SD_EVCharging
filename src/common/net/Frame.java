package common.net;
import java.nio.charset.StandardCharsets;

public record Frame(byte[] payload, byte lrc) {
  public static final byte STX = 0x02;
  public static final byte ETX = 0x03;
  public static byte computeLRC(byte[] data){ byte x=0; for(byte b: data) x^=b; return x; }
  public byte[] toBytes(){ byte[] d=payload; byte[] out=new byte[1+d.length+1+1]; out[0]=STX; System.arraycopy(d,0,out,1,d.length); out[1+d.length]=ETX; out[2+d.length]=lrc; return out; }
  public static Frame fromBytes(byte[] buf){ if(buf.length<4||buf[0]!=STX) throw new IllegalArgumentException(); int etx=buf.length-2; if(buf[etx]!=ETX) throw new IllegalArgumentException(); byte l=buf[buf.length-1]; byte[] d=java.util.Arrays.copyOfRange(buf,1,etx); if(computeLRC(d)!=l) throw new IllegalArgumentException(); return new Frame(d,l); }
  public static Frame ofJson(String json){ byte[] d=json.getBytes(StandardCharsets.UTF_8); return new Frame(d, computeLRC(d)); }
}