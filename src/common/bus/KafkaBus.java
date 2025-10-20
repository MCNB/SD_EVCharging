package common.bus;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public final class KafkaBus implements EventBus {
    private final boolean debug;
    private final Producer<String,String> producer;
    private final KafkaConsumer<String,String> consumer;

    private final Map<String, Consumer<JsonObject>> handlers = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(true);
    private final AtomicBoolean needsSubscribe = new AtomicBoolean(false);
    private volatile boolean subscribed = false;

    private final Thread poller;

    private KafkaBus(Properties p, boolean debug) {
        this.debug = debug;

        Properties prod = new Properties();
        prod.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, required(p,"kafka.bootstrapServers"));
        prod.put(ProducerConfig.CLIENT_ID_CONFIG,          p.getProperty("kafka.clientId","ev-app"));
        prod.put(ProducerConfig.ACKS_CONFIG,               "all");
        prod.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        prod.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "1");
        prod.put(ProducerConfig.LINGER_MS_CONFIG,          "0");
        prod.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,   StringSerializer.class.getName());
        prod.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.producer = new KafkaProducer<>(prod);

        Properties cons = new Properties();
        cons.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,  required(p,"kafka.bootstrapServers"));
        cons.put(ConsumerConfig.GROUP_ID_CONFIG,           required(p,"kafka.groupId"));
        cons.put(ConsumerConfig.CLIENT_ID_CONFIG,          p.getProperty("kafka.clientId","ev-app") + "-c");
        cons.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,   StringDeserializer.class.getName());
        cons.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        cons.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        cons.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,  p.getProperty("kafka.autoOffsetReset","earliest"));
        this.consumer = new KafkaConsumer<>(cons);

        // Hilo del poller
        this.poller = new Thread(this::pollLoop, "kafka-poller");
        this.poller.setDaemon(true);
        this.poller.start();
    }

    public static EventBus from(Properties p) {
        boolean enabled = Boolean.parseBoolean(p.getProperty("kafka.enabled","false"));
        boolean debug   = Boolean.parseBoolean(p.getProperty("kafka.debug","false"));
        if (!enabled) return new NoBus();

        System.out.println("[BUS] Kafka habilitado. bootstrap=" + p.getProperty("kafka.bootstrapServers")
                + " groupId=" + p.getProperty("kafka.groupId") + " debug=" + debug);

        // (Opcional) autocreación de topics
        if (Boolean.parseBoolean(p.getProperty("kafka.autoCreateTopics","true"))) {
            try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", required(p,"kafka.bootstrapServers")))) {
                String init = p.getProperty("kafka.topics.init","");
                if (init != null && !init.isBlank()) {
                    Set<String> existing = admin.listTopics().names().get();
                    List<NewTopic> toCreate = new ArrayList<>();
                    for (String t : init.split(",")) {
                        String tt = t.trim();
                        if (!tt.isEmpty() && !existing.contains(tt)) toCreate.add(new NewTopic(tt,1,(short)1));
                    }
                    if (!toCreate.isEmpty()) {
                        admin.createTopics(toCreate).all().get();
                        System.out.println("[BUS] Topics creados: " + toCreate);
                    }
                }
            } catch (Exception e) {
                System.out.println("[BUS] WARN creando topics: " + e.getMessage());
            }
        }

        return new KafkaBus(p, debug);
    }

    @Override
    public void publish(String topic, String key, JsonObject payload) {
        String value = payload.toString();
        if (debug) System.out.println("[BUS→KAFKA] topic=" + topic + " key=" + key + " value=" + value);
        try { producer.send(new ProducerRecord<>(topic, key, value)).get(); }
        catch (Exception e) { System.err.println("[BUS] ERROR publish: " + e.getMessage()); }
    }

    @Override
    public void subscribe(String topic, Consumer<JsonObject> handler) {
        handlers.put(topic, handler);
        needsSubscribe.set(true);       // ← NO tocamos el consumer aquí
        if (debug) System.out.println("[BUS] subscribe requested: " + handlers.keySet());
    }

    private void pollLoop() {
        try {
            while (running.get()) {
                // Aplicar (re)subscripciones SOLO desde este hilo
                if (needsSubscribe.getAndSet(false)) {
                    Set<String> topics = new HashSet<>(handlers.keySet());
                    if (!topics.isEmpty()) {
                        consumer.subscribe(topics);
                        subscribed = true;
                        System.out.println("[BUS] Subscribed topics: " + topics);
                    } else {
                        // Si se quitaran todos (no es tu caso), marcar no-subscrito
                        subscribed = false;
                    }
                }

                if (!subscribed) {
                    // Aún no hay topics: no llames a poll() o Kafka lanza excepción
                    try { Thread.sleep(50); } catch (InterruptedException ignore) {}
                    continue;
                }

                ConsumerRecords<String,String> recs = consumer.poll(Duration.ofMillis(250));
                if (recs.isEmpty()) continue;

                for (ConsumerRecord<String,String> r : recs) {
                    if (debug) System.out.println("[BUS←KAFKA] topic=" + r.topic() + " key=" + r.key() + " value=" + r.value());
                    Consumer<JsonObject> h = handlers.get(r.topic());
                    if (h == null) continue;
                    try {
                        JsonObject jo = JsonParser.parseString(r.value()).getAsJsonObject();
                        h.accept(jo);
                    } catch (Exception parse) {
                        System.err.println("[BUS] ERROR parse: " + parse.getMessage() + " value=" + r.value());
                    }
                }
            }
        } catch (WakeupException we) {
            // cierre
        } catch (Exception e) {
            if (running.get()) System.err.println("[BUS] pollLoop error: " + e.getMessage());
        } finally {
            try { consumer.close(); } catch (Exception ignore) {}
        }
    }

    @Override
    public void close() {
        running.set(false);
        try { consumer.wakeup(); } catch (Exception ignore) {}
        try { producer.flush(); producer.close(); } catch (Exception ignore) {}
        try { poller.join(1000); } catch (InterruptedException ignore) {}
    }

    private static boolean isBlank(String s){ return s==null || s.trim().isEmpty(); }
    private static String required(Properties p, String k){
        String v = p.getProperty(k);
        if (isBlank(v)) throw new IllegalArgumentException("Falta propiedad: " + k);
        return v.trim();
    }
}
