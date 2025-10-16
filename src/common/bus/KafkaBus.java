package common.bus;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;

public final class KafkaBus implements EventBus {
  private final KafkaProducer<String,String> producer;
  private final Properties base;
  private final String groupId;
  private volatile boolean running = true;

  public static EventBus from(Properties cfg) {
    if (!Boolean.parseBoolean(cfg.getProperty("kafka.enable","false"))) return new NoBus();
    try { return new KafkaBus(cfg); }
    catch (Exception e) { System.err.println("[KAFKA] disabled: "+e.getMessage()); return new NoBus(); }
  }

  private KafkaBus(Properties cfg) {
    base = new Properties();
    base.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, cfg.getProperty("kafka.bootstrap","localhost:9092"));
    base.put(ProducerConfig.CLIENT_ID_CONFIG, cfg.getProperty("kafka.clientId","central"));
    base.put(ProducerConfig.ACKS_CONFIG, "1");
    base.put(ProducerConfig.LINGER_MS_CONFIG, "10");
    base.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    base.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    this.groupId = cfg.getProperty("kafka.groupId","central-cmds-1");
    this.producer = new KafkaProducer<>(base);
  }

  @Override public void publish(String topic, JsonObject msg) {
    producer.send(new ProducerRecord<>(topic, msg.toString()), (md,ex) -> {
      if (ex!=null) System.err.println("[KAFKA] publish err: "+ex.getMessage());
    });
  }

  @Override public void subscribe(String topic, Consumer<JsonObject> handler) {
    Thread t = new Thread(() -> {
      Properties cp = new Properties();
      cp.putAll(base);
      cp.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
      cp.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      cp.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
      cp.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
      try (KafkaConsumer<String,String> c = new KafkaConsumer<>(cp)) {
        c.subscribe(Collections.singletonList(topic));
        while (running) {
          ConsumerRecords<String,String> recs = c.poll(Duration.ofMillis(500));
          for (ConsumerRecord<String,String> r: recs) {
            try { handler.accept(JsonParser.parseString(r.value()).getAsJsonObject()); }
            catch (Exception ex) { System.err.println("[KAFKA] bad msg: "+ex.getMessage()); }
          }
        }
      } catch (Exception e) {
        if (running) System.err.println("[KAFKA] consumer err: "+e.getMessage());
      }
    }, "kafka-sub-"+topic);
    t.setDaemon(true);
    t.start();
  }

  @Override public void close() {
    running = false;
    try { producer.flush(); producer.close(); } catch (Exception ignore){}
  }
}
