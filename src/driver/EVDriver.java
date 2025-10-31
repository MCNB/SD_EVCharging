package driver;

import com.google.gson.JsonObject;
import common.bus.EventBus;
import common.bus.KafkaBus;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static common.net.Wire.*; // obj(...) para construir JSON

public class EVDriver {

    private static int parseIntOr(String s, int def){ try{ return Integer.parseInt(s);}catch(Exception e){return def;} }

    public static void main(String[] args) throws Exception {
        String rutaConfig = "config/driver.config";
        Properties config = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(rutaConfig))) { config.load(in); }

        final String driverID     = config.getProperty("driver.id","D-001");
        final String filePath     = config.getProperty("driver.file","").trim();
        final int    authTimeoutMs= parseIntOr(config.getProperty("driver.authTimeoutMs","5000"), 5000);

        final String T_CMD        = config.getProperty("kafka.topic.cmd","ev.cmd.v1");
        final String T_SESSIONS   = config.getProperty("kafka.topic.sessions","ev.sessions.v1");

        EventBus bus = KafkaBus.from(config);
        System.out.println("[DRV][KAFKA] bootstrap=" + config.getProperty("kafka.bootstrap","(missing)") + " busImpl=" + bus.getClass().getSimpleName());
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { bus.close(); } catch(Exception ignore){} }, "drv-shutdown"));

        System.out.println("[DRV] Kafka driver=" + driverID + " topics: CMD=" + T_CMD + " SESS=" + T_SESSIONS);

        // Cola de eventos entrantes
        final LinkedBlockingQueue<JsonObject> q = new LinkedBlockingQueue<>();

        // Suscripción global a ev.sessions.v1: volcamos TODO a la cola y filtramos en waitFor(...)
        bus.subscribe(T_SESSIONS, msg -> {
            try {
                q.offer(msg);
                // Si quieres ver lo que llega, descomenta:
                // System.out.println("[DRV] evt@SESS: " + msg);
            } catch (Exception e) {
                System.err.println("[DRV] subscribe error: " + e.getMessage());
            }
        });

        // Entrada manual o por fichero (como antes)
        List<String> cps = null;
        if (!filePath.isEmpty()) {
            Path f = Path.of(filePath).toAbsolutePath().normalize();
            if (Files.isRegularFile(f)) {
                cps = new ArrayList<>();
                try (var lines = Files.lines(f, StandardCharsets.UTF_8)) {
                    lines.map(String::trim).filter(l -> !l.isEmpty() && !l.startsWith("#")).forEach(cps::add);
                }
                if (cps.isEmpty()) cps = null;
            }
        }

        if (cps != null) {
            int i=0, n=cps.size();
            for (String cp : cps) {
                i++;
                System.out.println("[DRV] ("+i+"/"+n+") solicitando "+cp+"…");
                flujoSolicitud(bus, T_CMD, q, driverID, cp, authTimeoutMs);
                Thread.sleep(4000); // como antes
            }
            System.out.println("[DRV] Fichero procesado. Fin.");
        } else {
            try (var br = new BufferedReader(new InputStreamReader(System.in))) {
                System.out.println("Conectado. Introduce CP-ID (Enter vacío para salir):");
                for (;;) {
                    System.out.print("CP-ID> ");
                    String cp = br.readLine();
                    if (cp==null || cp.isBlank()) break;
                    flujoSolicitud(bus, T_CMD, q, driverID, cp.trim(), authTimeoutMs);
                }
            }
        }
    }

    private static void flujoSolicitud(EventBus bus, String T_CMD, LinkedBlockingQueue<JsonObject> q,
                                       String driverID, String cp, int authTimeoutMs) throws Exception {
        // 1) Publica REQ_START por Kafka
        JsonObject req = obj("type","CMD","cmd","REQ_START","ts",System.currentTimeMillis(),
                     "driver",driverID,"cp",cp,"src","DRIVER");
        bus.publish(T_CMD, driverID, req);
        
        System.out.println("[DRV] CMD -> " + req);

        // 2) Espera AUTH dirigido a este driver
        JsonObject auth = waitFor(q, authTimeoutMs, m ->
            m.has("type") &&
            "AUTH".equals(m.get("type").getAsString()) &&
            driverID.equals(m.get("driver").getAsString()) &&
            cp.equalsIgnoreCase(m.get("cp").getAsString())

        );

        if (auth == null) {
            System.out.println("[DRV] DENEGADO: TIMEOUT esperando autorización");
            return;
        }
        if (!auth.has("ok") || !auth.get("ok").getAsBoolean()) {
            String reason = auth.has("reason") ? auth.get("reason").getAsString() : "DESCONOCIDO";
            System.out.println("[DRV] DENEGADO: " + reason);
            return;
        }

        String ses   = auth.has("session") ? auth.get("session").getAsString() : "(?)";
        String cpAns = auth.has("cp") ? auth.get("cp").getAsString() : cp;
        double price = auth.has("price") ? auth.get("price").getAsDouble() : 0.0;
        System.out.printf("[DRV] AUTORIZADO: sesión %s en %s (precio %.4f)%n", ses, cpAns, price);

        // 3) Espera cierre de sesión (SESSION_END de mi sesión)
        JsonObject ticket = waitFor(q, /*sin timeout*/ 0, m ->
            m.has("type") && "SESSION_END".equals(m.get("type").getAsString())
            && m.has("session") && ses.equals(m.get("session").getAsString())
        );

        if (ticket != null) {
            double kwh   = ticket.has("kwh") ? ticket.get("kwh").getAsDouble() : 0.0;
            double eur   = ticket.has("eur") ? ticket.get("eur").getAsDouble() : 0.0;
            String reason= ticket.has("reason") ? ticket.get("reason").getAsString() : "OK";
            System.out.printf("[DRV] TICKET session=%s cp=%s kWh=%.5f €=%.4f reason=%s%n",
                    ses, cpAns, kwh, eur, reason);
        }
    }

    // espera mensajes que cumplan el predicado; timeoutMs=0 ⇒ espera indefinidamente
    private static JsonObject waitFor(LinkedBlockingQueue<JsonObject> q, int timeoutMs,
                                      java.util.function.Predicate<JsonObject> pred) throws InterruptedException {
        long deadline = (timeoutMs > 0) ? System.currentTimeMillis() + timeoutMs : Long.MAX_VALUE;
        for (;;) {
            long wait = (timeoutMs > 0) ? Math.max(1, deadline - System.currentTimeMillis()) : 0L;
            JsonObject m = (timeoutMs > 0)
                    ? q.poll(wait, java.util.concurrent.TimeUnit.MILLISECONDS)
                    : q.take();
            if (m == null) return null;          // timeout
            if (pred.test(m)) return m;          // bingo
            // si no me sirve, la ignoro y sigo esperando
        }
    }
}
