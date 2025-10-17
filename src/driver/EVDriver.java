// src/driver/EVDriver.java
package driver;

import com.google.gson.JsonObject;
import common.bus.EventBus;
import common.bus.KafkaBus;

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static common.net.Wire.*;

public class EVDriver {

    // ======== util props ========
    private static int parseIntOr(String s, int def) {
        try { return Integer.parseInt(s); } catch (Exception e) { return def; }
    }

    // ======== contextos por petición ========
    static final class RequestCtx {
        final String cpId;
        final CountDownLatch startLatch = new CountDownLatch(1);
        final CountDownLatch endLatch   = new CountDownLatch(1);
        final AtomicReference<String> session = new AtomicReference<>(null);
        volatile String denyReason = null;
        volatile Double endKwh = null, endEur = null;
        volatile String endReason = "OK";
        RequestCtx(String cp){ this.cpId = cp; }
    }

    public static void main(String[] args) throws Exception {
        // --- carga config (igual que antes) ---
        String rutaConfig = "config/driver.config";
        Properties config = new Properties();
        Path ruta = Path.of(rutaConfig).toAbsolutePath().normalize();

        try (InputStream fichero = Files.newInputStream(ruta)) {
            config.load(fichero);
        } catch (NoSuchFileException e) {
            System.err.println("[DRV] No encuentro el fichero de configuración: " + ruta);
            System.exit(1); return;
        } catch (Exception e) {
            System.err.println("[DRV] Error leyendo configuración en " + ruta + ": " + e.getMessage());
            System.exit(1); return;
        }

        final String driverID  = config.getProperty("driver.id", "D-001");
        final String filePath  = config.getProperty("driver.file", "").trim();

        // timeouts (segundos) similares a tu semántica original
        final int startTimeoutSec = parseIntOr(config.getProperty("driver.startTimeoutSec"), 20);
        final int endTimeoutSec   = parseIntOr(config.getProperty("driver.endTimeoutSec"), 600);

        // --- Kafka topics (mismos nombres que en CENTRAL/ENGINE) ---
        final String T_CMD       = config.getProperty("kafka.topic.cmd",       "ev.cmd.v1");
        final String T_SESSIONS  = config.getProperty("kafka.topic.sessions",  "ev.sessions.v1");
        final String T_TELEMETRY = config.getProperty("kafka.topic.telemetry", "ev.telemetry.v1");

        System.out.println("[DRV] Config: " + ruta);
        System.out.printf("[DRV] Driver=%s | topics: CMD=%s, SESS=%s, TEL=%s%n",
                driverID, T_CMD, T_SESSIONS, T_TELEMETRY);

        // === Bus Kafka ===
        EventBus bus = KafkaBus.from(config);

        // Contexto “corriente” (1 servicio activo a la vez, como tu driver original)
        final AtomicReference<RequestCtx> current = new AtomicReference<>(null);

        // === Suscripciones ===
        bus.subscribe(T_SESSIONS, (JsonObject m) -> {
            try {
                if (!m.has("type")) return;
                String type = m.get("type").getAsString();

                switch (type) {
                    case "AUTH" -> {
                        // Central nos puede contestar denegación dirigida al driver
                        String d = m.has("driver") ? m.get("driver").getAsString() : "";
                        if (!driverID.equals(d)) return;
                        boolean ok = m.has("ok") && m.get("ok").getAsBoolean();
                        String cp = m.has("cp") ? m.get("cp").getAsString() : "";
                        RequestCtx ctx = current.get();
                        if (ctx == null || !ctx.cpId.equals(cp)) return;
                        if (!ok) {
                            ctx.denyReason = m.has("reason") ? m.get("reason").getAsString() : "DESCONOCIDO";
                            ctx.startLatch.countDown();
                        }
                    }
                    case "SESSION_START" -> {
                        // Autorizado
                        String d = m.has("driver") ? m.get("driver").getAsString() : "";
                        if (!driverID.equals(d)) return;
                        RequestCtx ctx = current.get();
                        if (ctx == null) return;
                        String cp = m.has("cp") ? m.get("cp").getAsString() : "";
                        if (!ctx.cpId.equals(cp)) return;

                        String sId = m.get("session").getAsString();
                        double price = m.has("price") ? m.get("price").getAsDouble() : Double.NaN;
                        ctx.session.set(sId);
                        System.out.printf("[DRV] AUTORIZADO: sesión %s en %s (precio %.4f)%n", sId, cp, price);
                        ctx.startLatch.countDown();
                    }
                    case "SESSION_END" -> {
                        // Ticket final
                        if (!m.has("session")) return;
                        RequestCtx ctx = current.get();
                        if (ctx == null) return;
                        String s = m.get("session").getAsString();
                        if (!s.equals(ctx.session.get())) return;

                        ctx.endKwh = m.has("kwh") ? m.get("kwh").getAsDouble() : 0.0;
                        ctx.endEur = m.has("eur") ? m.get("eur").getAsDouble() : 0.0;
                        ctx.endReason = m.has("reason") ? m.get("reason").getAsString() : "OK";
                        ctx.endLatch.countDown();
                    }
                }
            } catch (Exception e) {
                System.err.println("[DRV][SESSIONS] " + e.getMessage());
            }
        });

        // (Opcional) telemetrías en vivo durante la sesión
        bus.subscribe(T_TELEMETRY, (JsonObject m) -> {
            try {
                if (!m.has("type") || !"TEL".equals(m.get("type").getAsString())) return;
                RequestCtx ctx = current.get();
                if (ctx == null) return;
                String s = m.has("session") ? m.get("session").getAsString() : "";
                if (!s.equals(ctx.session.get())) return;
                double kwh = m.has("kwh") ? m.get("kwh").getAsDouble() : 0.0;
                double eur = m.has("eur") ? m.get("eur").getAsDouble() : 0.0;
                double pwr = m.has("power") ? m.get("power").getAsDouble() : Double.NaN;
                System.out.printf("[DRV] TEL session=%s power=%.2f kWh=%.5f €=%.4f%n", s, pwr, kwh, eur);
            } catch (Exception e) {
                System.err.println("[DRV][TEL] " + e.getMessage());
            }
        });

        // === Modo fichero o manual (igual que tu driver) ===
        List<String> cpList = null;
        if (!filePath.isEmpty()) {
            Path f = Path.of(filePath).toAbsolutePath().normalize();
            if (!Files.isRegularFile(f)) {
                System.err.println("[DRV] AVISO: driver.file apunta a un fichero que no existe: " + f);
            } else {
                System.out.println("[DRV] Modo fichero: " + f);
                cpList = new ArrayList<>();
                try (var lines = Files.lines(f, StandardCharsets.UTF_8)) {
                    lines.map(String::trim)
                         .filter(l -> !l.isEmpty() && !l.startsWith("#"))
                         .forEach(cpList::add);
                }
                if (cpList.isEmpty()) {
                    System.out.println("[DRV] El fichero está vacío. Paso a modo manual.");
                    cpList = null;
                }
            }
        }

        try (var br = new BufferedReader(new InputStreamReader(System.in))) {
            if (cpList != null) {
                // --- MODO FICHERO ---
                for (int i = 0; i < cpList.size(); i++) {
                    String cp = cpList.get(i).trim();
                    if (cp.isEmpty()) continue;

                    System.out.printf("[DRV] (%d/%d) -> REQ_START %s %s%n", i+1, cpList.size(), driverID, cp);
                    RequestCtx ctx = new RequestCtx(cp);
                    current.set(ctx);

                    JsonObject req = obj("type","CMD","cmd","REQ_START","driver",driverID,"cp",cp,"ts",System.currentTimeMillis());
                    bus.publish(T_CMD, driverID, req);

                    // Espera autorización (start o denegación)
                    boolean started = ctx.startLatch.await(startTimeoutSec, TimeUnit.SECONDS);
                    if (!started) {
                        System.out.println("[DRV] DENEGADO/Timeout esperando autorización.");
                        current.compareAndSet(ctx, null);
                        continue;
                    }
                    if (ctx.session.get() == null) {
                        System.out.println("[DRV] DENEGADO: " + (ctx.denyReason == null ? "DESCONOCIDO" : ctx.denyReason));
                        current.compareAndSet(ctx, null);
                        continue;
                    }

                    // Esperar ticket final
                    boolean ended = ctx.endLatch.await(endTimeoutSec, TimeUnit.SECONDS);
                    if (!ended) {
                        System.out.println("[DRV] Timeout esperando fin de sesión.");
                    } else {
                        System.out.printf("[DRV] TICKET session=%s cp=%s kWh=%.5f €=%.4f reason=%s%n",
                                ctx.session.get(), ctx.cpId,
                                safe(ctx.endKwh), safe(ctx.endEur), ctx.endReason);
                    }
                    current.compareAndSet(ctx, null);

                    System.out.println("[DRV] Esperando 4s antes del siguiente servicio…");
                    Thread.sleep(4000);
                }
                System.out.println("[DRV] Fichero procesado. Fin.");
            } else {
                // --- MODO MANUAL ---
                System.out.println("Conectado. Introduce CP-ID (Enter vacío para salir):");
                while (true) {
                    System.out.print("CP-ID> ");
                    String cp = br.readLine();
                    if (cp == null || cp.isBlank()) {
                        System.out.println("[DRV] Saliendo.");
                        break;
                    }
                    cp = cp.trim();

                    System.out.printf("[DRV] -> REQ_START %s %s%n", driverID, cp);
                    RequestCtx ctx = new RequestCtx(cp);
                    current.set(ctx);

                    JsonObject req = obj("type","CMD","cmd","REQ_START","driver",driverID,"cp",cp,"ts",System.currentTimeMillis());
                    bus.publish(T_CMD, driverID, req);

                    // Espera autorización
                    if (!ctx.startLatch.await(startTimeoutSec, TimeUnit.SECONDS)) {
                        System.out.println("[DRV] DENEGADO/Timeout esperando autorización.");
                        current.compareAndSet(ctx, null);
                        continue;
                    }
                    if (ctx.session.get() == null) {
                        System.out.println("[DRV] DENEGADO: " + (ctx.denyReason == null ? "DESCONOCIDO" : ctx.denyReason));
                        current.compareAndSet(ctx, null);
                        continue;
                    }

                    // Esperar ticket final
                    if (!ctx.endLatch.await(endTimeoutSec, TimeUnit.SECONDS)) {
                        System.out.println("[DRV] Timeout esperando fin de sesión.");
                    } else {
                        System.out.printf("[DRV] TICKET session=%s cp=%s kWh=%.5f €=%.4f reason=%s%n",
                                ctx.session.get(), ctx.cpId,
                                safe(ctx.endKwh), safe(ctx.endEur), ctx.endReason);
                    }
                    current.compareAndSet(ctx, null);
                }
            }
        } finally {
            try { bus.close(); } catch (Exception ignore) {}
        }
    }

    private static double safe(Double v){ return v==null?0.0:v; }
}
