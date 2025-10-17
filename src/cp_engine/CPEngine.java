package cp_engine;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
//import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Properties;

import com.google.gson.JsonObject;
import static common.net.Wire.*;

import common.bus.EventBus;
import common.bus.KafkaBus;
import common.bus.NoBus;

public class CPEngine {

    // ---- Estado simulado (igual que antes)
    static volatile boolean enchufado = false;
    static volatile boolean enMarcha = false;
    static volatile String sesionActiva = null;
    static volatile String cpIDActual = null;
    static volatile boolean healthy = true; // KO/OK

    static double potenciaKW;
    static int duracionDemoSec;

    // ---- Kafka
    private static EventBus bus = new NoBus();
    private static String T_TELEMETRY, T_SESSIONS, T_CMD;

    private static int parseIntOr(String s, int def) {
        try { return Integer.parseInt(s); }
        catch (Exception e) { return def; }
    }
    private static double parseDoubleOr(String s, double def) {
        try { return Double.parseDouble(s); }
        catch (Exception e) { return def; }
    }

    public static void main(String[] args) throws Exception {
        String rutaConfig = "config/engine.config";

        Properties config = new Properties();
        Path ruta = Path.of(rutaConfig).toAbsolutePath().normalize();

        try (InputStream fichero = Files.newInputStream(ruta)) {
            config.load(fichero);
        } catch (NoSuchFileException e) {
            System.err.println("[ENG] No encuentro el fichero de configuración: " + ruta);
            System.exit(1);
            return;
        } catch (Exception e) {
            System.err.println("[ENG] Error leyendo configuración en " + ruta + ": " + e.getMessage());
            System.exit(1);
            return;
        }

        // --- Config de simulador
        cpIDActual     = config.getProperty("engine.cpId", "CP-001");
        potenciaKW     = parseDoubleOr(config.getProperty("engine.potenciaKW","7.2"), 7.2);
        duracionDemoSec= parseIntOr(config.getProperty("engine.durationSec", "15"), 15);
        int puertoHealth = parseIntOr(config.getProperty("engine.healthPort","6100"), 6100);

        // --- Topics Kafka (por defecto compatibles con Central)
        T_TELEMETRY = config.getProperty("kafka.topic.telemetry","ev.telemetry.v1");
        T_SESSIONS  = config.getProperty("kafka.topic.sessions","ev.sessions.v1");
        T_CMD       = config.getProperty("kafka.topic.cmd","ev.cmd.v1");

        // --- Bus (Kafka)
        bus = KafkaBus.from(config);

        // --- Arrancar consola local y servidor de salud (JSON)
        iniciarConsola();
        iniciarHealthServer(puertoHealth);

        // --- Suscripción a comandos desde Central
        bus.subscribe(T_CMD, CPEngine::onKafkaCmd);

        System.out.println("[ENG] Listo. Esperando comandos en Kafka topic=" + T_CMD + " cp=" + cpIDActual);
        // Mantener proceso vivo
        Object lock = new Object();
        synchronized (lock) { lock.wait(); }
    }

    // =======================
    //   HANDLERS KAFKA CMD
    // =======================
    private static void onKafkaCmd(JsonObject m) {
        try {
            if (m==null || !m.has("type") || !"CMD".equals(m.get("type").getAsString())) return;

            String cmd = m.has("cmd") ? m.get("cmd").getAsString() : "";
            String cp  = m.has("cp")  ? m.get("cp").getAsString()  : "";

            if (!cpIDActual.equals(cp)) return; // ignorar cmds de otros CP

            switch (cmd) {
                case "START_SUPPLY" -> {
                    String ses = m.get("session").getAsString();
                    double precio = m.has("price") ? m.get("price").getAsDouble() : 0.35;
                    startSessionAsync(ses, cp, precio);
                }
                case "STOP_SUPPLY" -> {
                    System.out.println("[ENG] STOP_SUPPLY recibido");
                    enMarcha = false; // el bucle de sesión se cerrará solo y publicará SESSION_END
                }
                case "RESUME" -> {
                    // En este simulador, RESUME no hace nada especial en Engine.
                    System.out.println("[ENG] RESUME recibido (sin efecto en Engine)");
                }
                default -> System.out.println("[ENG] CMD desconocido: " + cmd);
            }
        } catch (Exception e) {
            System.err.println("[ENG][KAFKA] onCmd error: " + e.getMessage());
        }
    }

    // =======================
    //   SESIÓN DE SUMINISTRO
    // =======================
    private static void startSessionAsync(String sesionID, String cp, double precioPorKWh) {
        // Si ya hay sesión, cortamos la anterior
        if (sesionActiva != null && !sesionActiva.equals(sesionID)) {
            System.out.println("[ENG] Hay una sesión activa. Se detendrá al iniciar la nueva.");
            enMarcha = false;
            // el hilo anterior publicará su SESSION_END
        }
        new Thread(() -> runSession(sesionID, cp, precioPorKWh), "eng-session-"+sesionID).start();
    }

    private static void runSession(String sesionID, String cp, double precioPorKWh) {
        try {
            sesionActiva = sesionID;
            System.out.println("[ENG] START sesión " + sesionID + " en " + cp + " precio = " + precioPorKWh);
            System.out.println("[ENG] Esperando PLUG... (consola: PLUG/UNPLUG)");

            // Esperar a que se “enchufe” (o cancelación)
            while (!enchufado && sesionID.equals(sesionActiva)) {
                Thread.sleep(100);
            }
            if (!sesionID.equals(sesionActiva)) {
                System.out.println("[ENG] Sesión " + sesionID + " cancelada antes de iniciar.");
                return;
            }

            enMarcha = true;
            double kWh = 0.0, eur = 0.0;
            int seg = 0;
            long t0 = System.currentTimeMillis();
            String reason = "OK";

            while (enMarcha && enchufado && sesionID.equals(sesionActiva)) {
                long t = System.currentTimeMillis();
                if (t - t0 < 1000) {
                    Thread.sleep(10);
                    continue;
                }
                t0 = t;

                kWh += potenciaKW / 3600.0;
                eur = kWh * precioPorKWh;

                // TEL -> Kafka
                bus.publish(T_TELEMETRY, cp, obj(
                        "type","TEL","ts",System.currentTimeMillis(),
                        "session",sesionID,"cp",cp,
                        "pwr",potenciaKW,
                        "kwh",kWh,"eur",eur
                ));

                if (duracionDemoSec > 0 && ++seg >= duracionDemoSec) {
                    enMarcha = false;
                    reason = "DEMO_LIMIT";
                }
            }

            if (!enchufado && sesionID.equals(sesionActiva)) {
                reason = "UNPLUGGED";
            }
            if (!enMarcha && sesionID.equals(sesionActiva) && !"DEMO_LIMIT".equals(reason) && enchufado) {
                // STOP_SUPPLY explícito
                reason = "STOP_SUPPLY";
            }

            // SESSION_END -> Kafka
            bus.publish(T_SESSIONS, sesionID, obj(
                    "type","SESSION_END","ts",System.currentTimeMillis(),
                    "session",sesionID,"cp",cp,
                    "kwh",kWh,"eur",eur,"reason",reason
            ));

            System.out.println("[ENG] END sesión " + sesionID + " (" + reason + ")");
        } catch (Exception e) {
            System.err.println("[ENG] runSession error: " + e.getMessage());
        } finally {
            enMarcha = false;
            if (sesionID.equals(sesionActiva)) sesionActiva = null;
        }
    }

    // =======================
    //   HEALTH SERVER (JSON)
    // =======================
    public static void iniciarHealthServer (int port) {
        Thread t = new Thread(() -> {
            try (var ss = new ServerSocket(port)) {
                System.out.println("[ENG][HEALTH] Escuchando JSON en " + port);
                while (true) {
                    var c = ss.accept();
                    new Thread(() -> {
                        try (var in = new DataInputStream(c.getInputStream());
                             var out = new DataOutputStream(c.getOutputStream())) {
                            while (true){
                                JsonObject req = recv(in);     // <-- JSON del Monitor
                                if (req == null || !req.has("type")) continue;
                                String tpe = req.get("type").getAsString();
                                if ("PING".equalsIgnoreCase(tpe)) {
                                    send(out, obj("type","PONG","ok",healthy,"ts",System.currentTimeMillis()));
                                } else if ("STATUS".equalsIgnoreCase(tpe)) {
                                    send(out, obj("type","STATUS",
                                                  "ok",healthy,
                                                  "enchufado",enchufado,
                                                  "enMarcha",enMarcha,
                                                  "session", sesionActiva==null?"":sesionActiva,
                                                  "cp", cpIDActual,
                                                  "ts",System.currentTimeMillis()));
                                } else {
                                    send(out, obj("type","ERR","msg","UNKNOWN"));
                                }
                            }
                        }
                        catch (Exception ignore){}
                    }, "eng-health-client").start();
                }
            } catch (Exception e) {
                System.err.println("[ENG][HEALTH] ERROR: " + e.getMessage());
            }
        }, "eng-health");
        t.setDaemon(true);
        t.start();
    }

    // =======================
    //   CONSOLA LOCAL
    // =======================
    private static void iniciarConsola() {
        Thread consola = new Thread(() -> {
            try (var br = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))){
                System.out.println("[ENG] Comandos: PLUG | UNPLUG | STATUS | OK | KO");
                for (String line; (line = br.readLine()) != null; ){
                    switch (line.trim().toUpperCase()) {
                        case "PLUG" -> {
                            enchufado = true;
                            System.out.println("[ENG] PLUG");
                        }
                        case "UNPLUG" -> {
                            enchufado = false;
                            enMarcha = false;
                            System.out.println("[ENG] UNPLUG");
                        }
                        case "STATUS" -> {
                            System.out.println("[ENG] Enchufado=" + enchufado +
                                               " EnMarcha=" + enMarcha +
                                               " Sesión=" + (sesionActiva==null?"-":sesionActiva));
                        }
                        case "OK" -> {
                            healthy = true;
                            System.out.println("[ENG] Salud=OK");
                        }
                        case "KO" -> {
                            healthy = false;
                            System.out.println("[ENG] Salud=KO");
                        }
                        default -> {
                            if (!line.isBlank()) System.out.println("[ENG] Comando desconocido");
                        }
                    }
                }
            }
            catch (Exception ignore){}
        }, "engine-console");
        consola.setDaemon(true);
        consola.start();
    }
}
