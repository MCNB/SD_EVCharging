package common.bus;
import com.google.gson.JsonObject;
import java.util.function.Consumer;

public final class NoBus implements EventBus {
  public void publish(String t, JsonObject m) {}
  public void subscribe(String t, Consumer<JsonObject> h) {}
  public void close() {}
}
