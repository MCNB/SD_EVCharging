package central;

import java.io.*;
import java.net.BindException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonObject;
import static common.net.Wire.*;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpExchange;
import java.nio.charset.StandardCharsets;

import common.bus.EventBus;
import common.bus.NoBus;
import common.bus.KafkaBus;

import java.security.SecureRandom;
import java.util.Base64;
import java.util.Arrays;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import com.google.gson.JsonParser;



public class EVCentral {
    static class CPInfo {
        volatile String cpID, ubicacion;
        volatile double precio;
        volatile String estado = "ACTIVADO";
        volatile long lastHb = System.currentTimeMillis();
        volatile boolean ocupado = false;
        volatile boolean parado = false;
    }

    static class SesionInfo {
        final String sesionID, cpID, driverID;
        final long startUTC;
        volatile double kWhAccumulado = 0.0, eurAccumulado = 0.0;
        SesionInfo(String s, String cp, String drv){
            sesionID = s;
            cpID = cp;
            driverID = drv;
            startUTC = System.currentTimeMillis();
        }
    }

    private static class AuthResult {
        final boolean ok;
        final String key;
        final String reason;

        AuthResult(boolean ok, String key, String reason) {
            this.ok = ok;
            this.key = key;
            this.reason = reason;
        }
    }

    static String DB_URL, DB_USER, DB_PASS;

    private final static Map<String, CPInfo> cps = new ConcurrentHashMap<>();
    private static final JsonParser JSON = new JsonParser();
    private final Map<String, SesionInfo> sesiones = new ConcurrentHashMap<>();
    private final Map<String, String> cpSesionesActivas = new ConcurrentHashMap<>();
    private final java.util.Set<String> stopSolicitado = java.util.concurrent.ConcurrentHashMap.newKeySet(); //Para ver si el END viene de un STOP manual
    private final static java.util.Set<String> driversValidos = java.util.concurrent.ConcurrentHashMap.newKeySet();
    private final java.util.concurrent.ConcurrentMap<String,String> cpKeys = new java.util.concurrent.ConcurrentHashMap<>();
    private EventBus bus = new NoBus();
    private String T_TELEMETRY, T_SESSIONS, T_CMD;
    

    //Para evitar trolleos en el fichero de configuración
    //y se lea algo que no sea un número, evitando el NumberFormatException
    private static int parseIntOr (String s, int def) {
        try {
            return Integer.parseInt(s);
        }
        catch (Exception e) {
            return def;
        }
    }

    private void dbOpenSession (String sID, String cpID, String driverID, double precio){
        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             var st = cn.prepareCall("{call dbo.spOpenSession(?,?,?,?,?)}")) {
            st.setString(1, sID);
            st.setString(2, cpID);
            st.setString(3, driverID);
            st.setBigDecimal(4, java.math.BigDecimal.valueOf(precio).setScale(4, java.math.RoundingMode.HALF_UP));
            st.setTimestamp(5, java.sql.Timestamp.from(java.time.Instant.now()),
                    java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC")));
            st.execute();
        } catch (Exception e) { System.out.println("[CENTRAL][DB] spOpenSession("+sID+","+cpID+") ERROR: " + e.getMessage()); }
    }

    private void dbCloseSession (String sID, long tsMillis, String motivo, double kwh, double eur){
        if (DB_URL==null || DB_URL.isBlank()) return;
        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
            var st = cn.prepareCall("{call dbo.spCloseSession(?,?,?,?,?)}")) {
            st.setString(1, sID);
            st.setTimestamp(2, new java.sql.Timestamp(tsMillis),
                    java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC")));
            st.setString(3, motivo);
            st.setBigDecimal(4, java.math.BigDecimal.valueOf(kwh).setScale(6, java.math.RoundingMode.HALF_UP));
            st.setBigDecimal(5, java.math.BigDecimal.valueOf(eur).setScale(4, java.math.RoundingMode.HALF_UP));
            st.execute();
        } catch (Exception e) {
            System.out.println("[CENTRAL][DB] spCloseSession("+sID+") ERROR: " + e.getMessage());
        }
    }

    private void dbRecoverOpenSessions() {
        if (DB_URL==null || DB_URL.isBlank()) return;
        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
            var ps = cn.prepareStatement("""
                SELECT s.session_id, s.cp_id, s.driver_id, s.price_eur_kwh, s.t_start
                FROM dbo.[Session] s
                WHERE s.t_end IS NULL
            """);
            var rs = ps.executeQuery()) {

            int n=0;
            while (rs.next()) {
                String sesId  = rs.getString(1);
                String cpID   = rs.getString(2).toUpperCase(java.util.Locale.ROOT);
                String driver = rs.getString(3);

                // reconstruye in-memory
                var info = cps.computeIfAbsent(cpID, _ -> new CPInfo());
                synchronized (info) {
                    info.cpID   = cpID;
                    info.estado = "ESPERANDO_PLUG"; // o "DESCONECTADO" si no hay HB aún
                    info.ocupado= true;
                }
                var sInf = new SesionInfo(sesId, cpID, driver);
                sesiones.put(sesId, sInf);
                cpSesionesActivas.put(cpID, sesId);
                n++;
            }
            System.out.println("[CENTRAL][RECOVERY] Sesiones abiertas rehidratadas: " + n);
        } catch (Exception e) {
            System.err.println("[CENTRAL][RECOVERY] ERROR: " + e.getMessage());
        }
    }

    public static void main (String[] args) throws Exception {
        String rutaConfig = "config/central.config";

        Properties config = new Properties();
        Path ruta = Path.of(rutaConfig).toAbsolutePath().normalize();

        try(InputStream fichero = Files.newInputStream(ruta)) {
            config.load(fichero);
        } catch (NoSuchFileException e) {
            System.err.println("[CENTRAL] No encuentro el fichero de configuración: " + ruta);
            System.exit(1);
            return;
        } catch (Exception e) {
            System.err.println("[CENTRAL] Error leyendo configuración en " + ruta + ": " +e.getMessage());
            System.exit(1);
            return;
        }

        int port = parseIntOr(config.getProperty("central.listenPort"),5000);
        if (port < 1 || port > 65535) port = 5000;

        initDb(config);       // hace smoke-test si hay URL
        if (DB_URL == null || DB_URL.isBlank()) {
            System.out.println("[CENTRAL][DB] Desactivada (sin db.url). Se usará solo memoria.");
        } 
        else {
            dbLoadCPs();
            dbLoadDrivers();
        }

        boolean consolePanel = Boolean.parseBoolean(config.getProperty("central.consolepanel","false"));
        int httpPort = parseIntOr(config.getProperty("central.httpPort"),8080);
        
        var central = new EVCentral();

        central.T_TELEMETRY = config.getProperty("kafka.topic.telemetry","ev.telemetry.v1");
        central.T_SESSIONS  = config.getProperty("kafka.topic.sessions","ev.sessions.v1");
        central.T_CMD       = config.getProperty("kafka.topic.cmd","ev.cmd.v1");
        central.bus = KafkaBus.from(config);
        System.out.println("[CENTRAL][KAFKA] bootstrap=" + config.getProperty("kafka.bootstrap","(missing)") + " busImpl=" + central.bus.getClass().getSimpleName());

        // Suscripción a comandos (PAUSE/RESUME/STOP) vía Kafka ev.cmd.v1
        central.bus.subscribe(central.T_CMD, central::onKafkaCmd);
        // Suscripción a eventos llegan por Kafka desde ENGINE ev.telemetry.v1
        central.bus.subscribe(central.T_TELEMETRY, central::onKafkaTelemetry);
        // Suscripción a sesiones recibidas por Kafka (ev.sessions.v1)
        central.bus.subscribe(central.T_SESSIONS, central::onKafkaSessions);

        // Cierre limpio al terminar la JVM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { central.bus.close(); } catch(Exception ignore){} }, "shutdown-central"));

        if (consolePanel) {
            central.iniciarPanel();   
        }
        
        central.dbRecoverOpenSessions();
        central.iniciarWatchdog();
        central.iniciarHttpStatus(httpPort);
        

        try (ServerSocket server = new ServerSocket(port)) {
            System.out.println("[CENTRAL] Config: " + ruta); //debug
            System.out.println("[CENTRAL] Escuchando en puerto " + port);

            while (true) {
                Socket s = server.accept();
                new Thread(() -> central.atender(s)).start();
            }
        } catch (BindException e) {
            System.err.println("[CENTRAL] Puerto " + port + " en uso. Cambia 'central.listenPort' en " + ruta);
            throw e;
        }
    }

    private void atender(Socket s) {
        String cliente = s.getRemoteSocketAddress().toString();
        String remoteIp = s.getInetAddress().getHostAddress();
        System.out.println("[CENTRAL] Conexión " + cliente);

        try (DataInputStream in = new DataInputStream(s.getInputStream());
             DataOutputStream out = new DataOutputStream(s.getOutputStream())) {

            boolean authenticated = false;
            String cpIdAutenticado = null;

            while (true) {
                JsonObject msg = recv(in);
                String type = msg.has("type") ? msg.get("type").getAsString() : null;
                if (type == null) continue;

                // --- FASE DE AUTENTICACIÓN ---
                if (!authenticated) {
                    if ("AUTH_CP".equals(type)) {
                        String cpID   = msg.get("cp").getAsString();
                        String secret = msg.get("secret").getAsString();

                        if (cpID != null) {
                            cpID = cpID.toUpperCase(java.util.Locale.ROOT);
                        }

                        AuthResult ar = autenticarCpEnBd(cpID, secret, remoteIp);
                        if (ar.ok) {
                            // Enviamos AUTH_OK con la clave simétrica
                            send(out, obj("type","AUTH_OK",
                                          "ts",System.currentTimeMillis(),
                                          "cp",cpID,
                                          "key",ar.key));
                            authenticated = true;
                            cpIdAutenticado = cpID;
                            System.out.println("[CENTRAL] CP autenticado: " + cpID);
                        } else {
                            // Enviamos AUTH_ERR y cerramos conexión
                            send(out, obj("type","AUTH_ERR",
                                          "ts",System.currentTimeMillis(),
                                          "cp",cpID,
                                          "reason",ar.reason));
                            System.out.println("[CENTRAL] AUTH_CP rechazada para " + cpID +
                                               " reason=" + ar.reason);
                            break;
                        }
                    } else {
                        // Cualquier cosa que no sea AUTH_CP antes de autenticarse => error
                        send(out, obj("type","AUTH_ERR",
                                      "ts",System.currentTimeMillis(),
                                      "cp","?",
                                      "reason","MISSING_AUTH"));
                        System.out.println("[CENTRAL] Recibido " + type +
                                           " antes de AUTH_CP. Cerrando conexión " + cliente);
                        break;
                    }
                    continue; // seguimos al siguiente frame
                }

                // --- A PARTIR DE AQUÍ EL CP YA ESTÁ AUTENTICADO ---

                switch (type) {
                    // -------- Monitor: alta CP --------
                    case "REG_CP" -> {
                        String cpID = msg.get("cp").getAsString();
                        String loc  = msg.get("loc").getAsString();
                        double price= msg.get("price").getAsDouble();

                        // Normalizamos CP en mayúsculas
                        if (cpID != null) {
                            cpID = cpID.toUpperCase(java.util.Locale.ROOT);
                        }

                        CPInfo info = cps.computeIfAbsent(cpID, _ -> new CPInfo());
                        synchronized (info) {
                            info.cpID = cpID;
                            info.ubicacion = loc;
                            info.precio = price;
                            info.estado = "ACTIVADO";
                            info.lastHb = System.currentTimeMillis();
                        }
                        try { dbUpsertCP(cpID, loc, price); } catch (Exception ignore) {}
                    }
                    // -------- Monitor: heartbeat --------
                    case "HB" -> {
                        String cpID = msg.get("cp").getAsString().toUpperCase(java.util.Locale.ROOT);
                        boolean ok  = msg.get("ok").getAsBoolean();
                        CPInfo info = cps.get(cpID);
                        if (info == null) break;

                        info.lastHb = System.currentTimeMillis();
                        synchronized (info) {
                            if (!ok) info.estado = "AVERIADO";
                            else if (!info.parado) info.estado = "ACTIVADO";
                        }
                    }
                    default -> System.out.println("[CENTRAL] Tipo JSON desconocido: " + type);
                }
            }
        } catch (Exception e) {
            System.out.println("[CENTRAL] Conexión cerrada (" + cliente + "): " + e.getMessage());
        } finally {
            // aquí si quieres podrías loguear algo adicional al cierre
        }
    }

    private String estadoVisible (CPInfo info) {
        long ms = System.currentTimeMillis() - info.lastHb;
        boolean desconectado = ms > 3000;
        if (desconectado) {
            return "DESCONECTADO";
        }
        if ("AVERIADO".equals(info.estado)) {
            return "AVERIADO";
        }
        if (info.parado) {
            return "PARADO";
        }
        if (info.ocupado) {
            return "SUMINISTRANDO";
        }
        return "ACTIVADO";
    }

    private void iniciarPanel () {
        Thread t = new Thread(() -> {
            while (true) {
                try {
                    clearConsole();
                    System.out.println("CENTRAL — Panel CPs (1s)");
                    System.out.println("CP        | Estado          | Parado | Ocupado | lastHB(ms) | Precio | Sesión     | kWh     | EUR");
                    System.out.println("----------+-----------------+--------+---------+------------+--------+------------+---------+---------");

                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, CPInfo> e : cps.entrySet()) {
                        String cpID = e.getKey();
                        CPInfo info = e.getValue();

                        String ses = cpSesionesActivas.getOrDefault(cpID, "-");
                        String est = estadoVisible(info);
                        long lag = now - info.lastHb;

                        double kwh = 0.0, eur = 0.0;
                        var sInf = (ses.equals("-") ? null : sesiones.get(ses));
                        if (sInf != null) { 
                            kwh = sInf.kWhAccumulado; eur = sInf.eurAccumulado; 
                        }

                        System.out.printf("%-9s | %-15s | %-6s | %-7s | %10d | %6.2f | %-10s | %7.4f | %7.4f%n",
                            cpID, est, info.parado ? "SI" : "NO", info.ocupado ? "SI" : "NO",
                            lag, info.precio, ses, kwh, eur);
                    }

                    Thread.sleep(1000);
                } 
                catch (InterruptedException ie) { return; }
                catch (Exception ignore) {}
            }
        }, "panel-central");
        t.setDaemon(true);
        t.start();
    }

    private static void clearConsole (){
        try {
            String os = System.getProperty("os.name", "").toLowerCase();
            if (os.contains("win")) {                      
            new ProcessBuilder("cmd", "/c", "cls")
                .inheritIO()                              
                .start().waitFor();
            } 
            else {                                      
                System.out.print("\033[H\033[2J");
                System.out.flush();
            }
        } catch (Exception e) {
          
            for (int i = 0; i < 60; i++) System.out.println();
        }
    }

    private static void initDb (Properties config) {
        DB_URL  = config.getProperty("db.url");
        DB_USER = config.getProperty("db.user");
        DB_PASS = config.getProperty("db.pass");

        // Smoke test de conexión (1 vez al inicio)
        if (DB_URL != null && !DB_URL.isBlank()) {
            try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
                var st = cn.createStatement();
                var rs = st.executeQuery("SELECT @@VERSION AS v")) {
                if (rs.next()) {
                    System.out.println("[CENTRAL][DB] OK conectado. " + rs.getString("v"));
                }
            } 
            catch (Exception e) {
                System.err.println("[CENTRAL][DB] ERROR conectando: " + e.getMessage());
            }
        } 
        else {
            System.out.println("[CENTRAL][DB] Desactivada (sin db.url en config)");
        }
    }

    private void iniciarWatchdog () {
        Thread w = new Thread(() -> {
            while (true) {
                long now = System.currentTimeMillis();
                try {
                    for (Map.Entry<String, CPInfo> e : cps.entrySet()) {
                        CPInfo info = e.getValue();
                        long lag = now - info.lastHb;

                        // 3s sin latidos -> marcamos el CP como desconectado,
                        // pero NO cortamos la sesión en el CP ni enviamos STOP_SUPPLY.
                        if (lag > 3000) {
                            synchronized (info) {
                                if (!"DESCONECTADO".equals(info.estado)) {
                                    info.estado = "DESCONECTADO";
                                }
                            }
                        }
                    }
                    Thread.sleep(1000);
                } catch (InterruptedException ie) {
                    return;
                } catch (Exception ignore) {}
            }
        }, "watchdog-central");
        w.setDaemon(true);
        w.start();
    }

    private void iniciarHttpStatus (int httpPort) {
        try {
            HttpServer http = HttpServer.create(new InetSocketAddress(httpPort), 0);
            http.createContext("/api/status", this::handleStatusJson);
            http.createContext("/", this::handleStatusHtml);
            http.createContext("/cmd", this::handleCmd);
            http.setExecutor(java.util.concurrent.Executors.newCachedThreadPool());
            http.start();

            System.out.println("[CENTRAL][HTTP] Panel en http://127.0.0.1:" + httpPort + "/");
        } 
        catch (Exception e) {
            System.err.println("[CENTRAL][HTTP] No se pudo iniciar: " + e.getMessage());
        }
}

    private void handleStatusJson (HttpExchange ex) {
        try {
            String json = buildStatusJson();
            byte[] body = json.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
        } catch (Exception ignore) {} 
        finally {
            ex.close();
        }
    }

    private void handleStatusHtml (HttpExchange ex) {
        try {
            String html = """
                <html><head><meta charset="utf-8">
                <meta http-equiv="refresh" content="1">
                <style>
                    body{font-family:system-ui;margin:16px}
                    table{border-collapse:collapse;width:100%}
                    th,td{border:1px solid #ddd;padding:8px;text-align:left}
                    th{background:#f6f7f9;position:sticky;top:0}
                    tr.ACTIVADO{ background:#e8f7e8 }
                    tr.SUMINISTRANDO{ background:#c9f7c9 }
                    tr.PARADO{ background:#ffe9cc }
                    tr.AVERIADO{ background:#ffd6d6 }
                    tr.DESCONECTADO{ background:#eeeeee }
                    .muted{color:#666}
                </style>
                <title>EV Central - Panel</title></head><body>
                <h2>EV Central — Panel</h2>
                <table>
                    <thead>
                    <tr>
                        <th>CP</th><th>Ubicación</th><th>Estado</th><th>Parado</th><th>Ocupado</th>
                        <th>lastHB (ms)</th><th>Precio</th><th>Sesión</th><th>Driver</th><th>kWh</th><th>€</th>
                    </tr>
                    </thead>
                    <tbody>
                    %ROWS%
                    </tbody>
                </table>
                <p class="muted">Actualiza cada 1s. Colores: Verde=Activado/Suministrando, Naranja=Parado, Rojo=Averiado, Gris=Desconectado.</p>
                </body></html>
            """;
            String rows = buildStatusRowsHtml();
            byte[] body = html.replace("%ROWS%", rows).getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "text/html; charset=utf-8");
            ex.sendResponseHeaders(200, body.length);
            ex.getResponseBody().write(body);
        } 
        catch (Exception ignore) {} 
        finally {
            ex.close();
        }
    }

    private String buildStatusJson () {
        long now = System.currentTimeMillis();
        var sb = new StringBuilder();
        sb.append("{\"items\":[");
        boolean first = true;
        for (var e : cps.entrySet()) {
            String cpID = e.getKey();
            CPInfo info = e.getValue();

            String estado; boolean ocupado; boolean parado; long last;
            double precio;
            synchronized (info) {               // ← snapshot coherente
                estado = info.estado;
                ocupado= info.ocupado;
                parado = info.parado;
                last   = info.lastHb;
                precio = info.precio;
            }

            String sesion = cpSesionesActivas.getOrDefault(cpID, "-");
            var sInf = sesiones.get(sesion);
            double kwh = (sInf != null) ? sInf.kWhAccumulado : 0.0;
            double eur = (sInf != null) ? sInf.eurAccumulado : 0.0;

            long lag = now - info.lastHb;

            if (!first) sb.append(',');
            first = false;
            sb.append("{")
            .append("\"cp\":\"").append(cpID).append("\",")
            .append("\"estado\":\"").append(estado).append("\",")
            .append("\"parado\":").append(info.parado).append(',')
            .append("\"ocupado\":").append(info.ocupado).append(',')
            .append("\"lastHbMs\":").append(lag).append(',')
            .append("\"precio\":").append(String.format(java.util.Locale.ROOT,"%.4f", info.precio)).append(',')
            .append("\"sesion\":\"").append("-".equals(sesion)?"":sesion).append("\",")
            .append("\"kwh\":").append(String.format(java.util.Locale.ROOT,"%.5f", kwh)).append(',')
            .append("\"eur\":").append(String.format(java.util.Locale.ROOT,"%.4f", eur))
            .append("}");
        }
        sb.append("]}");
        return sb.toString();
    }

    private String toHtml(String s){
        if (s == null) return "";
        return s.replace("&","&amp;").replace("<","&lt;").replace(">","&gt;");
    }

    private String buildStatusRowsHtml() {
        long now = System.currentTimeMillis();
        StringBuilder sb = new StringBuilder();

        for (var e : cps.entrySet()) {
            String cpID = e.getKey();
            CPInfo info = e.getValue();

            String sesion = cpSesionesActivas.getOrDefault(cpID, "-");
            var sInf = sesiones.get(sesion);
            double kwh = (sInf != null) ? sInf.kWhAccumulado : 0.0;
            double eur = (sInf != null) ? sInf.eurAccumulado : 0.0;
            String driver = (sInf != null) ? sInf.driverID : "";

            String est = estadoVisible(info);
            long lag = now - info.lastHb;

            String encCp = java.net.URLEncoder.encode(cpID, StandardCharsets.UTF_8);
            String acc = "<a href='/cmd?op=PAUSE&cp="+encCp+"'>PAUSE</a> | "
                + "<a href='/cmd?op=RESUME&cp="+encCp+"'>RESUME</a> | "
                + "<a href='/cmd?op=STOP&cp="+encCp+"'>STOP</a>";

            sb.append("<tr class='").append(est).append("'>")
            .append("<td>").append(toHtml(cpID)).append("</td>")
            .append("<td>").append(toHtml(info.ubicacion!=null?info.ubicacion:"")).append("</td>")
            .append("<td>").append(est).append("</td>")
            .append("<td>").append(info.parado ? "SI" : "NO").append("</td>")
            .append("<td>").append(info.ocupado ? "SI" : "NO").append("</td>")
            .append("<td>").append(lag).append("</td>")
            .append("<td>").append(String.format(java.util.Locale.ROOT,"%.4f", info.precio)).append("</td>")
            .append("<td>").append("-".equals(sesion) ? "" : toHtml(sesion)).append("</td>")
            .append("<td>").append(toHtml(driver)).append("</td>")
            .append("<td>").append(String.format(java.util.Locale.ROOT,"%.5f", kwh)).append("</td>")
            .append("<td>").append(String.format(java.util.Locale.ROOT,"%.4f", eur)).append("</td>")
            .append("<td>").append(acc).append("</td>")
            .append("</tr>");
        }
        return sb.toString();
    }

    private static void dbLoadCPs() {
        if (DB_URL == null || DB_URL.isBlank()) {
            return;
        }
        try (var conn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
            var st = conn.createStatement();
            var rs = st.executeQuery("SELECT cp_id, location_tag, price_eur_kwh FROM ChargingPoint")) {
                int n = 0;
                while (rs.next()) {
                    String cp = rs.getString(1), ubicacion = rs.getString(2);
                    double precio = rs.getBigDecimal(3).doubleValue();
                    CPInfo info = cps .computeIfAbsent(cp, _ -> new CPInfo());
                    info.cpID = cp;
                    info.ubicacion = ubicacion;
                    info.precio = precio;
                    info.estado = "DESCONECTADO";
                    info.ocupado = false;
                    info.parado = false;
                    info.lastHb = 0L;
                    n++;
                }
                System.out.println("[CENTRAL][DB] CPs precargados: " + n);                
        } catch (Exception e) {
            System.err.println("[CENTRAL][DB] dbLoader ERROR: " + e.getMessage());
        }
    }
    
    private AuthResult autenticarCpEnBd(String cpId, String secret, String remoteIp) {
        // Si no hay BD configurada, aceptamos todo y solo generamos la clave
        if (DB_URL == null || DB_URL.isBlank()) {
            String key = getOrCreateCpKey(cpId);
            System.out.println("[CENTRAL][AUTH_CP] WARNING: sin DB_URL, aceptando CP=" + cpId +
                               " ip=" + remoteIp);
            return new AuthResult(true, key, null);
        }

        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             var ps = cn.prepareStatement(
                     "SELECT secret, status FROM EV_CP_REGISTRY WHERE cp_id = ?")) {

            ps.setString(1, cpId);
            try (var rs = ps.executeQuery()) {
                if (!rs.next()) {
                    System.out.println("[CENTRAL][AUTH_CP] FAIL cp=" + cpId +
                                       " ip=" + remoteIp + " reason=NOT_REGISTERED");
                    return new AuthResult(false, null, "NOT_REGISTERED");
                }

                String dbSecret = rs.getString("secret");
                String status   = rs.getString("status");

                if (!"ACTIVO".equalsIgnoreCase(status)) {
                    System.out.println("[CENTRAL][AUTH_CP] FAIL cp=" + cpId +
                                       " ip=" + remoteIp + " reason=NOT_ACTIVE(" + status + ")");
                    return new AuthResult(false, null, "NOT_ACTIVE");
                }

                if (!dbSecret.equals(secret)) {
                    System.out.println("[CENTRAL][AUTH_CP] FAIL cp=" + cpId +
                                       " ip=" + remoteIp + " reason=BAD_SECRET");
                    return new AuthResult(false, null, "BAD_SECRET");
                }
            }

            String key = getOrCreateCpKey(cpId);
            System.out.println("[CENTRAL][AUTH_CP] OK cp=" + cpId + " ip=" + remoteIp +
                               " key=" + key);
            return new AuthResult(true, key, null);

        } catch (Exception e) {
            System.err.println("[CENTRAL][AUTH_CP] ERROR cp=" + cpId + " ip=" + remoteIp +
                               " msg=" + e.getMessage());
            return new AuthResult(false, null, "DB_ERROR");
        }
    }

    private static void dbLoadDrivers() {
        if (DB_URL==null || DB_URL.isBlank()) return;
        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             var st = cn.createStatement();
             var rs = st.executeQuery("SELECT driver_id FROM dbo.Driver")) {
            while (rs.next()) driversValidos.add(rs.getString(1));
            System.out.println("[CENTRAL][DB] Drivers precargados: "+driversValidos.size());
        } 
        catch (Exception e) {
            System.err.println("[CENTRAL][DB] dbLoadDrivers ERROR: " + e.getMessage()); 
        }
    }

    private static void dbUpsertCP(String cpID, String loc, double price) {
        if (DB_URL == null || DB_URL.isBlank()) return;
        final String sql = """
            MERGE dbo.ChargingPoint AS t
            USING (SELECT ? AS cp_id, ? AS location_tag, ? AS price_eur_kwh) AS s
                (cp_id, location_tag, price_eur_kwh)
            ON (t.cp_id = s.cp_id)
            WHEN MATCHED THEN
                UPDATE SET t.location_tag = s.location_tag,
                        t.price_eur_kwh = s.price_eur_kwh
            WHEN NOT MATCHED THEN
                INSERT (cp_id, location_tag, price_eur_kwh)
                VALUES (s.cp_id, s.location_tag, s.price_eur_kwh);
        """;
        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
            var ps = cn.prepareStatement(sql)) {
            ps.setString(1, cpID);
            ps.setString(2, loc);
            ps.setBigDecimal(3, java.math.BigDecimal.valueOf(price).setScale(4, java.math.RoundingMode.HALF_UP));
            ps.executeUpdate();
            System.out.println("[CENTRAL][DB] CP upsert: " + cpID + " (" + loc + ", " + price + ")");
        } catch (Exception e) {
            System.err.println("[CENTRAL][DB] dbUpsertCP ERROR: " + e.getMessage());
        }
    }

    private boolean ensureDriver(String driverID) {
        if (driverID == null || driverID.isBlank()) return false; // en memoria ya conocido
        if (driversValidos.contains(driverID)) return true; // si no hay BD, lo aceptamos y cacheamos
        if (DB_URL == null || DB_URL.isBlank()) {
            driversValidos.add(driverID);
            System.out.println("[CENTRAL] (MEM) Nuevo driver: " + driverID);
            return true;
        }

        String sql = """
            MERGE dbo.Driver AS t
            USING (SELECT ? AS driver_id) AS s
            ON (t.driver_id = s.driver_id)
            WHEN NOT MATCHED THEN INSERT(driver_id) VALUES (s.driver_id);
        """;

        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
            var ps = cn.prepareStatement(sql)) {
            ps.setString(1, driverID);
            ps.executeUpdate();
            driversValidos.add(driverID);
            System.out.println("[CENTRAL][DB] Driver asegurado: " + driverID);
            return true;
        } 
        catch (Exception e) {
            System.err.println("[CENTRAL][DB] ensureDriver ERROR: " + e.getMessage());
            // En caso de fallo de BD, acepta en memoria
            driversValidos.add(driverID);
            return true;
        }
    }

    private void handleCmd(com.sun.net.httpserver.HttpExchange ex) { 
        try {
            var q = ex.getRequestURI().getQuery();
            String op=null, cp=null;
            if (q != null) {
                for (String kv : q.split("&")) {
                    int i = kv.indexOf('=');
                    if (i>0) {
                        String k = java.net.URLDecoder.decode(kv.substring(0,i), StandardCharsets.UTF_8);
                        String v = java.net.URLDecoder.decode(kv.substring(i+1), StandardCharsets.UTF_8);
                        if ("op".equalsIgnoreCase(k)) op=v.toUpperCase();
                        if ("cp".equalsIgnoreCase(k)) cp=v;
                    }
                }
            }
            String msg = "ERR missing op/cp";
            if (op!=null && cp!=null) {
                switch (op) {
                    case "PAUSE"  -> msg = pauseCp(cp);
                    case "RESUME" -> msg = resumeCp(cp);
                    case "STOP"   -> msg = stopCp(cp);
                    default       -> msg = "ERR unknown op";
                }
            }

            String target = "/?msg=" + java.net.URLEncoder.encode(msg, java.nio.charset.StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Location", target);
            ex.sendResponseHeaders(303, -1);
        } 
        catch (Exception ignore) {} 
        finally { 
            ex.close(); 
        }
    }
    
    private String pauseCp(String cpID){
        applyPauseLocal(cpID);
        markStopRequested(cpID);

        com.google.gson.JsonObject inner = obj("type","CMD","src","CENTRAL","ts",System.currentTimeMillis(),
                                               "cmd","STOP_SUPPLY","cp",cpID);
        com.google.gson.JsonObject enc = encryptForCp(cpID, inner);
        bus.publish(T_CMD, cpID, enc);

        return "ACK PAUSE " + cpID + " (STOP_SUPPLY publicado)";
    }

    private String resumeCp(String cpID){
        applyResumeLocal(cpID);

        com.google.gson.JsonObject inner = obj("type","CMD","src","CENTRAL","ts",System.currentTimeMillis(),
                                               "cmd","RESUME","cp",cpID);
        com.google.gson.JsonObject enc = encryptForCp(cpID, inner);
        bus.publish(T_CMD, cpID, enc);

        return "ACK RESUME " + cpID + " (RESUME publicado)";
    }

    private String stopCp(String cpID){
        markStopRequested(cpID);

        com.google.gson.JsonObject inner = obj("type","CMD","src","CENTRAL","ts",System.currentTimeMillis(),
                                               "cmd","STOP_SUPPLY","cp",cpID);
        com.google.gson.JsonObject enc = encryptForCp(cpID, inner);
        bus.publish(T_CMD, cpID, enc);

        return "ACK STOP " + cpID + " (STOP_SUPPLY publicado)";
    }

    private void onKafkaCmd(com.google.gson.JsonObject m) {
        try {
            if (!m.has("type") || !"CMD".equals(m.get("type").getAsString())) return;

            // --- Anti-eco: Central publica a T_CMD para el Engine; no debe re-procesarse a sí misma
            String cmd = m.get("cmd").getAsString();
            String src = m.has("src") ? m.get("src").getAsString() : "";
            if ("CENTRAL".equals(src)) return;

            // --- NO REQ_START: STOP/PAUSE/RESUME, etc. ---
            if (!"REQ_START".equals(cmd)) {
                switch (cmd) {
                    case "REQ_STOP":
                    case "STOP":
                    case "STOP_SUPPLY": {
                        String session = m.has("session") ? m.get("session").getAsString() : null;
                        String cpID    = m.has("cp") ? m.get("cp").getAsString() : null;
                        if (cpID != null) cpID = cpID.toUpperCase(java.util.Locale.ROOT);

                        // intenta deducir sesión si no viene
                        if (session == null && cpID != null) {
                            session = cpSesionesActivas.get(cpID);
                        }
                        if (session == null && m.has("driver")) {
                            String driverId = m.get("driver").getAsString();
                            for (var e : sesiones.entrySet()) {
                                var s = e.getValue();
                                if (driverId.equals(s.driverID)) {
                                    session = e.getKey();
                                    cpID = s.cpID;
                                    break;
                                }
                            }
                        }

                        if (session != null && cpID != null) {
                            if (m.has("driver")) {
                                String driverId = m.get("driver").getAsString();
                                bus.publish(T_SESSIONS, driverId,
                                    obj("type","STOP_ACK","ts",System.currentTimeMillis(),
                                        "ok",true,"session",session,"cp",cpID,"src","CENTRAL"));
                            }
                            com.google.gson.JsonObject innerCmd = obj("type","CMD","ts",System.currentTimeMillis(),"src","CENTRAL",
                                          "cmd","STOP_SUPPLY","session",session,"cp",cpID);
                            com.google.gson.JsonObject enc = encryptForCp(cpID, innerCmd);
                            bus.publish(T_CMD, cpID, enc);
                        } else {
                            if (m.has("driver")) {
                                String driverId = m.get("driver").getAsString();
                                bus.publish(T_SESSIONS, driverId,
                                    obj("type","STOP_ACK","ts",System.currentTimeMillis(),
                                        "ok",false,"reason","SESSION_NOT_FOUND","src","CENTRAL"));
                            }
                        }
                        return;
                    }

                    case "REQ_PAUSE":
                    case "PAUSE": {
                        String session = m.has("session") ? m.get("session").getAsString() : null;
                        String cpID    = m.has("cp") ? m.get("cp").getAsString() : null;
                        if (cpID != null) cpID = cpID.toUpperCase(java.util.Locale.ROOT);
                        if (session == null && cpID != null) session = cpSesionesActivas.get(cpID);
                        if (session != null && cpID != null) {
                            com.google.gson.JsonObject innerCmd = obj("type","CMD","ts",System.currentTimeMillis(),"src","CENTRAL",
                                          "cmd","PAUSE_SUPPLY","session",session,"cp",cpID);
                            com.google.gson.JsonObject enc = encryptForCp(cpID, innerCmd);
                            bus.publish(T_CMD, cpID, enc);
                        }
                        return;
                    }

                    case "REQ_RESUME":
                    case "RESUME": {
                        String session = m.has("session") ? m.get("session").getAsString() : null;
                        String cpID    = m.has("cp") ? m.get("cp").getAsString() : null;
                        if (cpID != null) cpID = cpID.toUpperCase(java.util.Locale.ROOT);
                        if (session == null && cpID != null) session = cpSesionesActivas.get(cpID);
                        if (session != null && cpID != null) {
                            com.google.gson.JsonObject innerCmd = obj("type","CMD","ts",System.currentTimeMillis(),"src","CENTRAL",
                                          "cmd","RESUME_SUPPLY","session",session,"cp",cpID);
                            com.google.gson.JsonObject enc = encryptForCp(cpID, innerCmd);
                            bus.publish(T_CMD, cpID, enc);
                        }
                        return;
                    }

                    default:
                        return; // desconocido para Central
                }
            }

            // --- DRIVER -> CENTRAL: REQ_START ---
            String driverId = m.get("driver").getAsString();
            String cpID     = m.get("cp").getAsString();
            if (cpID != null) cpID = cpID.toUpperCase(java.util.Locale.ROOT);

            // 1) Valida driver
            if (!ensureDriver(driverId)) {
                bus.publish(T_SESSIONS, driverId,
                    obj("type","AUTH","ts",System.currentTimeMillis(),
                        "driver",driverId,"cp",cpID,"ok",false,"reason","DRIVER_INVALIDO","src","CENTRAL"));
                return;
            }

            // 2) Valida CP
            CPInfo info = cps.get(cpID);
            if (info == null) {
                bus.publish(T_SESSIONS, driverId,
                    obj("type","AUTH","ts",System.currentTimeMillis(),
                        "driver",driverId,"cp",cpID,"ok",false,"reason","CP_DESCONOCIDO","src","CENTRAL"));
                return;
            }

            // 3) Reservar y construir mensajes bajo lock; publicar fuera
            String sesId = null;
            double price = 0.0;
            com.google.gson.JsonObject authMsg = null, startCmd = null, sessStart = null;

            synchronized (info) {
                if (!"ACTIVADO".equals(info.estado)) {
                    authMsg = obj("type","AUTH","ts",System.currentTimeMillis(),
                                "driver",driverId,"cp",cpID,"ok",false,"reason",info.estado,"src","CENTRAL");
                } else if (info.parado) {
                    authMsg = obj("type","AUTH","ts",System.currentTimeMillis(),
                                "driver",driverId,"cp",cpID,"ok",false,"reason","PARADO","src","CENTRAL");
                } else if (info.ocupado) {
                    authMsg = obj("type","AUTH","ts",System.currentTimeMillis(),
                                "driver",driverId,"cp",cpID,"ok",false,"reason","OCUPADO","src","CENTRAL");
                } else {
                    sesId = "S-" + System.currentTimeMillis();
                    price = info.precio;
                    info.ocupado = true;
                    info.estado  = "ESPERANDO_PLUG"; // ← clave para la UX/flow

                    var sInf = new SesionInfo(sesId, cpID, driverId);
                    sesiones.put(sesId, sInf);
                    cpSesionesActivas.put(cpID, sesId);

                    try { dbOpenSession(sesId, cpID, driverId, price); } catch (Exception ignore) {}

                    long now = System.currentTimeMillis();
                    authMsg   = obj("type","AUTH","ts",now,"driver",driverId,"cp",cpID,
                                    "ok",true,"session",sesId,"price",price,"src","CENTRAL");

                    startCmd  = obj("type","CMD","ts",now,"src","CENTRAL",
                                    "cmd","START_SUPPLY","session",sesId,"cp",cpID,"price",price);

                    sessStart = obj("type","SESSION_START","ts",now,"src","CENTRAL",
                                    "session",sesId,"cp",cpID,"driver",driverId,"price",price);
                }
            } // fin synchronized(info)

            // 4) Publicaciones FUERA del lock
            if (authMsg != null) bus.publish(T_SESSIONS, driverId, authMsg);
            if (startCmd != null && sessStart != null) {
                com.google.gson.JsonObject encStart = encryptForCp(cpID, startCmd);
                bus.publish(T_CMD,      cpID,  encStart);
                // T_SESSIONS hacia el driver sigue en claro.
                bus.publish(T_SESSIONS, sesId, sessStart);
            }

        } catch (Exception e) {
            System.err.println("[CENTRAL][KAFKA] onCmd: " + e.getMessage());
        }
    }

    private void onKafkaTelemetry(com.google.gson.JsonObject m) {
        try {
            // --- Soportar mensajes cifrados desde el CP (envoltorio ENC) ---
            if (m.has("type") && "ENC".equals(m.get("type").getAsString())) {
                com.google.gson.JsonObject inner = decryptFromCp(m);
                if (inner == null) return;      // no se ha podido descifrar
                m = inner;                      // seguimos trabajando con el JSON interno
            }

            // --- A partir de aquí es el mensaje TEL "normal" ---
            if (!m.has("type") || !"TEL".equals(m.get("type").getAsString())) return;

            String ses  = m.get("session").getAsString();
            String cpID = m.get("cp").getAsString()
                            .toUpperCase(java.util.Locale.ROOT);

            double kwh = m.get("kwh").getAsDouble();
            double eur = m.get("eur").getAsDouble();

            var sInf = sesiones.get(ses);
            if (sInf != null) {
                sInf.kWhAccumulado = kwh;
                sInf.eurAccumulado = eur;
            }

            var info = cps.get(cpID);
            if (info != null) {
                synchronized (info) {
                    if (!"SUMINISTRANDO".equals(info.estado)) {
                        info.estado = "SUMINISTRANDO";
                    }
                }
            }

        } catch (Exception e) {
            System.err.println("[CENTRAL][KAFKA] TEL: " + e.getMessage());
        }
    }

    private void onKafkaSessions(com.google.gson.JsonObject m) {
        try {
            // --- Soportar mensajes cifrados desde el CP (envoltorio ENC) ---
            if (m.has("type") && "ENC".equals(m.get("type").getAsString())) {
                com.google.gson.JsonObject inner = decryptFromCp(m);
                if (inner == null) return;      // no se ha podido descifrar
                m = inner;                      // seguimos con el JSON interno
            }

            // --- A partir de aquí es el SESSION_END "normal" ---
            if (!m.has("type")) return;
            if (!"SESSION_END".equals(m.get("type").getAsString())) return;

            String sesId = m.get("session").getAsString();
            String cpID  = m.get("cp").getAsString();
            if (cpID != null) {
                cpID = cpID.toUpperCase(java.util.Locale.ROOT);
            }

            long tsEnd   = m.has("ts")     ? m.get("ts").getAsLong()      : System.currentTimeMillis();
            double kwh   = m.has("kwh")    ? m.get("kwh").getAsDouble()   : 0.0;
            double eur   = m.has("eur")    ? m.get("eur").getAsDouble()   : 0.0;
            String reason= m.has("reason") ? m.get("reason").getAsString(): "OK";

            boolean eraSTOP = stopSolicitado.remove(sesId);
            if (eraSTOP && "OK".equals(reason)) {
                reason = "STOP_REQUESTED";
            }

            sesiones.remove(sesId);
            cpSesionesActivas.remove(cpID, sesId);

            var info = cps.get(cpID);
            if (info != null) {
                synchronized (info) {
                    info.ocupado = false;
                    if (!info.parado && !"AVERIADO".equals(info.estado)) {
                        info.estado = "ACTIVADO";
                    }
                }
            }

            try {
                dbCloseSession(sesId, tsEnd, reason, kwh, eur);
            } catch (Exception ignore) {}

        } catch (Exception e) {
            System.err.println("[CENTRAL][KAFKA] SESSION_END: " + e.getMessage());
        }
    }

    private void applyPauseLocal(String cpID){
        CPInfo info = cps.get(cpID);
        if (info == null) return;
        synchronized (info) { info.parado = true; }
    }

    private void applyResumeLocal(String cpID){
        CPInfo info = cps.get(cpID);
        if (info == null) return;
        synchronized (info) {
            info.parado = false;
            if (!"AVERIADO".equals(info.estado)) info.estado = "ACTIVADO";
        }
    }
  
    private void markStopRequested(String cpID){
        String sId = cpSesionesActivas.get(cpID);
        if (sId != null) stopSolicitado.add(sId);
    }

    private String getOrCreateCpKey(String cpId) {
        return cpKeys.computeIfAbsent(cpId, id -> generarClaveSimetrica());
    }

    private String generarClaveSimetrica() {
        byte[] buf = new byte[32]; // 256 bits
        new java.security.SecureRandom().nextBytes(buf);
        return java.util.Base64.getEncoder().encodeToString(buf);
    }

        // ---------- Cifrado simétrico AES/GCM para CPs ----------

    private static String aesEncrypt(String plainText, String keyB64) throws Exception {
        byte[] keyBytes = Base64.getDecoder().decode(keyB64);
        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");

        // IV de 96 bits recomendado para GCM
        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(128, iv);
        cipher.init(Cipher.ENCRYPT_MODE, key, spec);
        byte[] cipherText = cipher.doFinal(plainText.getBytes(StandardCharsets.UTF_8));

        // empaquetamos IV + cipherText y lo codificamos en Base64
        byte[] out = new byte[iv.length + cipherText.length];
        System.arraycopy(iv, 0, out, 0, iv.length);
        System.arraycopy(cipherText, 0, out, iv.length, cipherText.length);

        return Base64.getEncoder().encodeToString(out);
    }

    private static String aesDecrypt(String cipherB64, String keyB64) throws Exception {
        byte[] all = Base64.getDecoder().decode(cipherB64);
        if (all.length < 13) throw new IllegalArgumentException("cipher too short");

        byte[] iv          = Arrays.copyOfRange(all, 0, 12);
        byte[] cipherBytes = Arrays.copyOfRange(all, 12, all.length);

        byte[] keyBytes = Base64.getDecoder().decode(keyB64);
        SecretKeySpec key = new SecretKeySpec(keyBytes, "AES");

        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(128, iv);
        cipher.init(Cipher.DECRYPT_MODE, key, spec);

        byte[] plainBytes = cipher.doFinal(cipherBytes);
        return new String(plainBytes, StandardCharsets.UTF_8);
    }

    /**
     * Envuelve un mensaje "interno" (CMD, etc.) para un CP en un sobre cifrado:
     * {
     *   "type":"ENC","src":"CENTRAL","cp":cpID,"ts":...,"payload":"base64(iv+cipher)"
     * }
     */
    private com.google.gson.JsonObject encryptForCp(String cpID, com.google.gson.JsonObject inner) {
        String key = cpKeys.get(cpID);
        if (key == null) {
            System.err.println("[CENTRAL][ENC] No hay clave para CP " + cpID + ". Enviando sin cifrar.");
            return inner; // fallback: protocolo antiguo
        }
        try {
            String cipherB64 = aesEncrypt(inner.toString(), key);
            com.google.gson.JsonObject env = obj(
                    "type","ENC",
                    "src","CENTRAL",
                    "ts",System.currentTimeMillis(),
                    "cp",cpID,
                    "payload",cipherB64
            );
            return env;
        } catch (Exception e) {
            System.err.println("[CENTRAL][ENC] Error cifrando para CP " + cpID + ": " + e.getMessage());
            return inner; // fallback
        }
    }

    /**
     * Descifra un mensaje "ENC" procedente de un CP.
     * Devuelve el JSON interno (CMD/TEL/SESSION_END) o null si algo falla.
     */
    private com.google.gson.JsonObject decryptFromCp(com.google.gson.JsonObject m) {
        try {
            if (!m.has("cp") || !m.has("payload")) {
                System.err.println("[CENTRAL][DEC] Mensaje ENC sin cp/payload");
                return null;
            }
            String cpID = m.get("cp").getAsString().toUpperCase(java.util.Locale.ROOT);
            String key  = cpKeys.get(cpID);
            if (key == null) {
                System.err.println("[CENTRAL][DEC] No hay clave para CP " + cpID);
                return null;
            }
            String cipherB64 = m.get("payload").getAsString();
            String plainJson = aesDecrypt(cipherB64, key);
            return JSON.parse(plainJson).getAsJsonObject();
        } catch (Exception e) {
            System.err.println("[CENTRAL][DEC] Error descifrando mensaje ENC: " + e.getMessage());
            return null;
        }
    }

}

