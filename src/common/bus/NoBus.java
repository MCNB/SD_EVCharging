package common.bus;

import com.google.gson.JsonObject;
import java.util.function.Consumer;

public final class NoBus implements EventBus {

    @Override
    public void publish(String topic, String key, JsonObject payload) {
        System.out.println("[BUS:NOOP] publish topic=" + topic + " key=" + key + " payload=" + payload);
    }

    @Override
    public void subscribe(String topic, java.util.function.Consumer<JsonObject> handler) {
        System.out.println("[BUS:NOOP] subscribe topic=" + topic + " (sin efecto)");
    }
}
