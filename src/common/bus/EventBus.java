package common.bus;

import com.google.gson.JsonObject;
import java.util.function.Consumer;

public interface EventBus extends AutoCloseable {
    void publish(String topic, String key, JsonObject payload);
    void subscribe(String topic, Consumer<JsonObject> handler);
    @Override default void close() throws Exception {}
}
