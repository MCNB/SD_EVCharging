package common.net;

import com.google.gson.*;
import java.io.*;
import java.nio.charset.StandardCharsets;

public final class Wire {
  private static final Gson GSON = new GsonBuilder().disableHtmlEscaping().create();

  public static void send(DataOutputStream out, JsonObject obj) throws IOException {
    byte[] bytes = GSON.toJson(obj).getBytes(StandardCharsets.UTF_8);
    out.writeInt(bytes.length);
    out.write(bytes);
    out.flush();
  }

  public static JsonObject recv(DataInputStream in) throws IOException {
    int len = in.readInt();
    if (len < 0 || len > 10 * 1024 * 1024) throw new IOException("Invalid frame size " + len);
    byte[] buf = new byte[len];
    in.readFully(buf);
    return JsonParser.parseString(new String(buf, StandardCharsets.UTF_8)).getAsJsonObject();
  }

  // Helper para construir objetos rápido: Wire.obj("k","v","n",123,"ok",true)
  public static JsonObject obj(Object... kv) {
    JsonObject o = new JsonObject();
    for (int i = 0; i < kv.length; i += 2) {
      String k = (String) kv[i];
      Object v = kv[i + 1];
      if (v == null) o.add(k, JsonNull.INSTANCE);
      else if (v instanceof Number n) o.addProperty(k, n);
      else if (v instanceof Boolean b) o.addProperty(k, b);
      else if (v instanceof String s) o.addProperty(k, s);
      else if (v instanceof JsonElement je) o.add(k, je);
      else o.add(k, GSON.toJsonTree(v));
    }
    return o;
  }
}

