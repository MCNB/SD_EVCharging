package common.bus;
import com.google.gson.JsonObject;
import java.util.function.Consumer;

public interface EventBus {
  void publish(String topic, JsonObject msg);
  void subscribe(String topic, Consumer<JsonObject> handler);
  void close();
}
