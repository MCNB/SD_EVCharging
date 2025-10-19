package cp_engine;

import com.google.gson.JsonObject;
import common.bus.EventBus;
import common.bus.KafkaBus;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.util.Properties;

import static common.net.Wire.*;

public class CPEngine {

    static volatile boolean enchufado = false;
    static volatile boolean enMarcha = false;
    static volatile String sesionActiva = null;
    static volatile String cpIDActual = null;
    static volatile boolean healthy = true;

    static double potenciaKW;
    static int duracionDemoSec;

    private static int parseIntOr(String s, int def){ try{ return Integer.parseInt(s);}catch(Exception e){return def;} }
    private static double parseDoubleOr(String s, double def){ try{ return Double.parseDouble(s);}catch(Exception e){return def;} }

    public static void main(String[] args) throws Exception {
        String rutaConfig = "config/engine.config";
        Properties cfg = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(rutaConfig))) { cfg.load(in); }

        int puertoHealth = parseIntOr(cfg.getProperty("engine.healthPort","6100"),6100);
        String cpId      = cfg.getProperty("engine.cpId","CP-001");
        potenciaKW       = parseDoubleOr(cfg.getProperty("engine.potenciaKW","7.2"),7.2);
        duracionDemoSec  = parseIntOr(cfg.getProperty("engine.durationSec","15"),15);

        final String T_CMD       = cfg.getProperty("kafka.topic.cmd","ev.cmd.v1");
        final String T_TELEMETRY = cfg.getProperty("kafka.topic.telemetry","ev.telemetry.v1");
        final String T_SESSIONS  = cfg.getProperty("kafka.topic.sessions","ev.sessions.v1");

        EventBus bus = KafkaBus.from(cfg);

        iniciarHealthServer(puertoHealth);
        iniciarConsola();

        System.out.println("[ENG] cp=" + cpId + " Kafka topics: CMD=" + T_CMD + " TEL=" + T_TELEMETRY + " SESS=" + T_SESSIONS);

        // Suscribimos comandos para mi CP
        bus.subscribe(T_CMD, m -> {
            try {
                if (!m.has("type") || !"CMD".equals(m.get("type").getAsString())) return;
                if (!m.has("cp") || !cpId.equals(m.get("cp").getAsString())) return;

                String cmd = m.has("cmd") ? m.get("cmd").getAsString() : "";
                switch (cmd) {
                    case "START_SUPPLY" -> {
                        String ses = m.get("session").getAsString();
                        double price = m.has("price") ? m.get("price").getAsDouble() : 0.0;
                        startSupply(bus, T_TELEMETRY, T_SESSIONS, cpId, ses, price);
                    }
                    case "STOP_SUPPLY" -> {
                        System.out.println("[ENG] STOP_SUPPLY recibido");
                        enMarcha = false;
                    }
                    case "RESUME" -> {
                        // opcional
                        System.out.println("[ENG] RESUME (sin efecto en demo)");
                    }
                }
            } catch (Exception e) {
                System.err.println("[ENG] onCmd ERROR: " + e.getMessage());
            }
        });

        // El hilo principal queda vivo
        Thread.currentThread().join();
    }

    private static void startSupply(EventBus bus, String T_TELEMETRY, String T_SESSIONS,
                                    String cp, String sesionID, double precio) {
        new Thread(() -> {
            try {
                sesionActiva = sesionID;
                cpIDActual = cp;

                System.out.println("[ENG] START sesión " + sesionID + " en " + cp + " precio=" + precio);
                System.out.println("[ENG] Esperando PLUG...");
                while (!enchufado && sesionID.equals(sesionActiva)) {
                    Thread.sleep(100);
                }
                if (!sesionID.equals(sesionActiva)) return; // abortada

                enMarcha = true;
                double kWh = 0.0, eur = 0.0;
                int seg = 0;
                long t0 = System.currentTimeMillis();

                while (enMarcha && enchufado && sesionID.equals(sesionActiva)) {
                    long t = System.currentTimeMillis();
                    if (t - t0 < 1000) { Thread.sleep(10); continue; }
                    t0 = t;

                    kWh += potenciaKW / 3600.0;
                    eur  = kWh * precio;

                    JsonObject tel = obj("type","TEL","ts",t,
                                         "session",sesionID,"cp",cp,
                                         "power",potenciaKW,"kwh",kWh,"eur",eur);
                    bus.publish(T_TELEMETRY, sesionID, tel);

                    if (duracionDemoSec > 0 && ++seg >= duracionDemoSec) {
                        enMarcha = false;
                    }
                }

                // Fin de sesión: publicamos SESSION_END
                JsonObject end = obj("type","SESSION_END","ts",System.currentTimeMillis(),
                                     "session",sesionID,"cp",cp,"kwh",kWh,"eur",eur,"reason","OK");
                bus.publish(T_SESSIONS, sesionID, end);

                System.out.println("[ENG] -> SESSION_END " + sesionID + " " + cp);
            } catch (Exception e) {
                System.err.println("[ENG] startSupply ERR: " + e.getMessage());
            } finally {
                enMarcha = false;
                sesionActiva = null;
                cpIDActual = null;
            }
        }, "eng-supply-" + sesionID).start();
    }

    // --- Tu health server tal cual ---
    public static void iniciarHealthServer (int port) {
        Thread t = new Thread(() -> {
            try (var ss = new ServerSocket(port)) {
                System.out.println("[ENG][HEALTH] Escuchando en " + port);
                while (true) {
                    var c = ss.accept();
                    new Thread(() -> {
                        try (var in = new DataInputStream(c.getInputStream());
                             var out = new DataOutputStream(c.getOutputStream())) {
                            while (true){
                                String msg = in.readUTF();
                                if("PING".equalsIgnoreCase(msg)) {
                                    out.writeUTF(healthy ? "OK" : "KO");
                                } else {
                                    out.writeUTF("UNKNOWN");
                                }
                            }
                        } catch (Exception ignore){}
                    }, "eng-health-client").start();
                }
            } catch (Exception e) {
                System.err.println("[ENG][HEALTH] ERROR: " + e.getMessage());
            }
        }, "eng-health");
        t.setDaemon(true);
        t.start();
    }

    // --- Tu consola tal cual ---
    private static void iniciarConsola() {
        Thread consola = new Thread(() -> {
            try (var br = new BufferedReader(new InputStreamReader(System.in))){
                System.out.println("[ENG] Comandos: PLUG | UNPLUG | STATUS | OK | KO");
                for (String line; (line = br.readLine()) != null; ){
                    switch (line.trim().toUpperCase()) {
                        case "PLUG"   -> { enchufado = true;  System.out.println("[ENG] PLUG"); }
                        case "UNPLUG" -> { enchufado = false; enMarcha=false; System.out.println("[ENG] UNPLUG"); }
                        case "STATUS" -> System.out.println("[ENG] Enchufado=" + enchufado + " EnMarcha=" + enMarcha + " Sesión=" + sesionActiva);
                        case "OK"     -> { healthy = true;  System.out.println("[ENG] Salud = OK"); }
                        case "KO"     -> { healthy = false; System.out.println("[ENG] Salud = KO"); }
                        default       -> { if (!line.isBlank()) System.out.println("[ENG] Comando desconocido"); }
                    }
                }
            } catch (Exception ignore){}
        }, "engine-console");
        consola.setDaemon(true);
        consola.start();
    }
}
