package cp_engine;

import com.google.gson.JsonObject;
import common.bus.EventBus;
import common.bus.KafkaBus;

import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.*;
import java.util.Properties;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import java.nio.charset.StandardCharsets;


import static common.net.Wire.*;

public class CPEngine {

    static volatile boolean enchufado = false;
    static volatile boolean enMarcha = false;
    static volatile String sesionActiva = null;
    static volatile String cpIDActual = null;
    static volatile boolean healthy = true;
    static volatile String cpCfgId = null;  // cp configurado para mostrar en el panel cuando no haya sesión

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
        boolean consolePanel = Boolean.parseBoolean(cfg.getProperty("engine.consolepanel","false"));
        int httpPort = parseIntOr(cfg.getProperty("engine.httpPort","8081"),8081);

        final String T_CMD       = cfg.getProperty("kafka.topic.cmd","ev.cmd.v1");
        final String T_TELEMETRY = cfg.getProperty("kafka.topic.telemetry","ev.telemetry.v1");
        final String T_SESSIONS  = cfg.getProperty("kafka.topic.sessions","ev.sessions.v1");

        EventBus bus = KafkaBus.from(cfg);

        cpCfgId = cpId;

        iniciarHealthServer(puertoHealth);
        if (consolePanel) iniciarConsola(); 
        iniciarHttpPanel(httpPort);

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

    static void startSupply(EventBus bus, String T_TELEMETRY, String T_SESSIONS,String cp, String sesionID, double precio) {
        new Thread(() -> {
            final String thisSession = sesionID;
            double kWh = 0.0, eur = 0.0;
            try {
                // -- Estado de sesión --
                sesionActiva = thisSession;
                cpIDActual   = cp;
                enMarcha     = false;
                enchufado    = false; // ← exige PLUG nuevo SIEMPRE

                // ACK temprano de espera
                bus.publish(T_SESSIONS, thisSession,
                    obj("type","WAITING_PLUG","ts",System.currentTimeMillis(),
                        "session",thisSession,"cp",cp,"src","ENGINE"));

                System.out.println("[ENG] Esperando PLUG...");
                while (!enchufado && thisSession.equals(sesionActiva)) {
                    Thread.sleep(100);
                }

                // Si nos “pisan” la sesión, CIERRA explícitamente
                if (!thisSession.equals(sesionActiva)) {
                    bus.publish(T_SESSIONS, thisSession,
                        obj("type","SESSION_END","ts",System.currentTimeMillis(),
                            "session",thisSession,"cp",cp,"kwh",0.0,"eur",0.0,
                            "reason","ABORTED_SUPERSEDED","src","ENGINE"));
                    return;
                }

                // (Opcional) CHARGING_STARTED
                bus.publish(T_SESSIONS, thisSession,
                    obj("type","CHARGING_STARTED","ts",System.currentTimeMillis(),
                        "session",thisSession,"cp",cp,"src","ENGINE"));

                enMarcha = true;

                int seg = 0;
                long t0 = System.currentTimeMillis();
                while (enMarcha && enchufado && thisSession.equals(sesionActiva)) {
                    long t = System.currentTimeMillis();
                    if (t - t0 < 1000) { Thread.sleep(10); continue; }
                    t0 = t;

                    kWh += potenciaKW / 3600.0;
                    eur  = kWh * precio;

                    bus.publish(T_TELEMETRY, thisSession,
                        obj("type","TEL","ts",t,"session",thisSession,"cp",cp,
                            "power",potenciaKW,"kwh",kWh,"eur",eur,"src","ENGINE"));

                    if (duracionDemoSec > 0 && ++seg >= duracionDemoSec) enMarcha = false;
                }

                bus.publish(T_SESSIONS, thisSession,
                    obj("type","SESSION_END","ts",System.currentTimeMillis(),
                        "session",thisSession,"cp",cp,"kwh",kWh,"eur",eur,
                        "reason","OK","src","ENGINE"));

            } catch (Exception e) {
                System.err.println("[ENG] startSupply ERR: " + e.getMessage());
            } finally {
                enMarcha = false;
                enchufado = false;   // ← CRÍTICO: no heredar PLUG
                sesionActiva = null;
                cpIDActual   = null;
            }
        }, "eng-supply-" + sesionID).start();
    }

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

    private static void iniciarHttpPanel(int httpPort) {
        try {
            HttpServer http = HttpServer.create(new java.net.InetSocketAddress(httpPort), 0);
            http.createContext("/", CPEngine::handlePanel);  // antes handleStatusHtml
            http.createContext("/cmd", CPEngine::handleCmd);
            http.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
            http.start();
            System.out.println("[ENG][HTTP] Panel en http://127.0.0.1:" + httpPort + "/");
        } catch (Exception e) {
            System.err.println("[ENG][HTTP] No se pudo iniciar: " + e.getMessage());
        }
    }

    private static void handlePanel(com.sun.net.httpserver.HttpExchange ex) {
        try {
            String cp  = (cpIDActual != null) ? cpIDActual : (cpCfgId != null ? cpCfgId : "");
            String okPill = healthy ? "<span class='pill ok'>OK</span>" : "<span class='pill ko'>KO</span>";
            String ses = (sesionActiva != null) ? sesionActiva : "";
            String now = new java.text.SimpleDateFormat("dd/MM/yyyy, HH:mm:ss").format(new java.util.Date());

            String html = """
                <html><head><meta charset="utf-8">
                <style>
                body{font-family:system-ui;margin:16px}
                .row{display:flex;gap:12px;align-items:center;flex-wrap:wrap}
                .pill{padding:6px 10px;border-radius:14px;border:1px solid #ddd}
                .ok{background:#e8f7e8} .ko{background:#ffd6d6}
                .btn{display:inline-block;padding:8px 12px;border:1px solid #bbb;border-radius:8px;text-decoration:none;color:#000}
                .btn:hover{background:#f6f7f9}
                table{margin-top:12px;border-collapse:collapse;min-width:360px}
                td{padding:6px 8px;border-bottom:1px solid #eee}
                .k{color:#666}
                </style>
                <title>EV Engine - Panel</title></head><body>
                <h2>EV Engine — Panel</h2>
                <div class="row">
                    <a class="btn" href="/cmd?op=PLUG">PLUG</a>
                    <a class="btn" href="/cmd?op=UNPLUG">UNPLUG</a>
                    <a class="btn" href="/cmd?op=OK">OK</a>
                    <a class="btn" href="/cmd?op=KO">KO</a>
                </div>
                <table>
                    <tr><td class="k">CP</td><td>%s</td></tr>
                    <tr><td class="k">Healthy</td><td>%s</td></tr>
                    <tr><td class="k">Enchufado</td><td>%s</td></tr>
                    <tr><td class="k">En marcha</td><td>%s</td></tr>
                    <tr><td class="k">Sesión</td><td>%s</td></tr>
                    <tr><td class="k">Potencia kW</td><td>%s</td></tr>
                    <tr><td class="k">Duración demo (s)</td><td>%s</td></tr>
                    <tr><td class="k">ts</td><td>%s</td></tr>
                </table>
                </body></html>
                """.formatted(cp, okPill, String.valueOf(enchufado), String.valueOf(enMarcha),
                            ses, String.valueOf(potenciaKW), String.valueOf(duracionDemoSec), now);

            byte[] body = html.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
        } catch (Exception ignore) {
        } finally {
            ex.close();
        }
    }

    private static void handleCmd(com.sun.net.httpserver.HttpExchange ex) {
        try {
            String q = ex.getRequestURI().getQuery();
            String op = null;
            if (q != null) {
                for (String kv : q.split("&")) {
                    int i = kv.indexOf('=');
                    if (i > 0) {
                        String k = java.net.URLDecoder.decode(kv.substring(0, i), java.nio.charset.StandardCharsets.UTF_8);
                        String v = java.net.URLDecoder.decode(kv.substring(i + 1), java.nio.charset.StandardCharsets.UTF_8);
                        if ("op".equalsIgnoreCase(k)) op = v.trim().toUpperCase();
                    }
                }
            }
            if (op != null) {
                switch (op) {
                    case "PLUG"   -> { enchufado = true; }
                    case "UNPLUG" -> { enchufado = false; enMarcha = false; }
                    case "OK"     -> { healthy = true; }
                    case "KO"     -> { healthy = false; }
                    case "STATUS" -> { /* compat: no-op */ }
                    default -> { /* ignorar desconocidas */ }
                }
            }
            ex.getResponseHeaders().add("Location", "/");
            ex.getResponseHeaders().add("Cache-Control","no-store");
            ex.sendResponseHeaders(303, -1); // See Other -> panel
        } catch (Exception ignore) {
        } finally {
            ex.close();
        }
    }

}
