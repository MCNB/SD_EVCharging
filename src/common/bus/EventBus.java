package common.bus;
import com.google.gson.JsonObject;
import java.util.function.Consumer;

public interface EventBus {
  // Nuevo: publicar con clave
  void publish(String topic, String key, JsonObject msg);

  // Compatibilidad: si no pasas clave, usa null
  default void publish(String topic, JsonObject msg) {
    publish(topic, null, msg);
  }

  void subscribe(String topic, java.util.function.Consumer<JsonObject> handler);
  void close();
}
