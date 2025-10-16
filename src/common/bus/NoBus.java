package common.bus;
import com.google.gson.JsonObject;
import java.util.function.Consumer;

public final class NoBus implements EventBus {
  @Override public void publish(String t, String key, JsonObject m) {}
  @Override public void publish(String t, JsonObject m) {} // opcional; ya lo cubre el default
  @Override public void subscribe(String t, Consumer<JsonObject> h) {}
  @Override public void close() {}
}

