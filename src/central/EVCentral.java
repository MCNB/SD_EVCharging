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


public class EVCentral {
    static class CPInfo {
        String cpID, ubicacion;
        double precio;
        String estado = "ACTIVADO";
        long lastHb = System.currentTimeMillis();
        boolean ocupado = false;
        boolean parado = false;
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

    static String DB_URL, DB_USER, DB_PASS;

    private final static Map<String, CPInfo> cps = new ConcurrentHashMap<>();
    private final Map<String, SesionInfo> sesiones = new ConcurrentHashMap<>();
    private final Map<String, String> cpSesionesActivas = new ConcurrentHashMap<>();
    private final java.util.Set<String> stopSolicitado = java.util.concurrent.ConcurrentHashMap.newKeySet(); //Para ver si el END viene de un STOP manual
    private final static java.util.Set<String> driversValidos = java.util.concurrent.ConcurrentHashMap.newKeySet();
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

    private void dbCloseSession (String sID, String motivo, double kwh, double eur){
        try (var cn = java.sql.DriverManager.getConnection(DB_URL, DB_USER, DB_PASS);
             var st = cn.prepareCall("{call dbo.spCloseSession(?,?,?,?,?)}")) {
            st.setString(1, sID);
            st.setTimestamp(2, java.sql.Timestamp.from(java.time.Instant.now()),
                    java.util.Calendar.getInstance(java.util.TimeZone.getTimeZone("UTC")));
            st.setString(3, motivo);
            st.setBigDecimal(4, java.math.BigDecimal.valueOf(kwh).setScale(6, java.math.RoundingMode.HALF_UP));
            st.setBigDecimal(5, java.math.BigDecimal.valueOf(eur).setScale(4, java.math.RoundingMode.HALF_UP));
            st.execute();
        } catch (Exception e) { System.out.println("[CENTRAL][DB] spCloseSession("+sID+") ERROR: " + e.getMessage()); }
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

        // Suscripción a comandos (PAUSE/RESUME/STOP) vía Kafka
        central.bus.subscribe(central.T_CMD, central::onKafkaCmd);
        // Suscripción a eventos que ahora llegan por Kafka desde ENGINE
        central.bus.subscribe(central.T_TELEMETRY, central::onKafkaTelemetry);
        // IMPORTANTE: para cerrar sesiones recibidas por Kafka (ev.sessions.v1)
        central.bus.subscribe(central.T_SESSIONS, central::onKafkaSessions);

        // Cierre limpio al terminar la JVM
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { try { central.bus.close(); } catch(Exception ignore){} }, "shutdown-central"));

        if (consolePanel) {
            central.iniciarPanel();   
        }
    
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
        System.out.println("[CENTRAL] Conexión " + cliente);

        try (DataInputStream in = new DataInputStream(s.getInputStream());
             DataOutputStream out = new DataOutputStream(s.getOutputStream())) {

            while (true) {
                JsonObject msg = recv(in);
                String type = msg.has("type") ? msg.get("type").getAsString() : null;
                if (type == null) continue;

                switch (type) {
                    // -------- Monitor: alta CP --------
                    case "REG_CP" -> {
                        String cpID = msg.get("cp").getAsString();
                        String loc  = msg.get("loc").getAsString();
                        double price= msg.get("price").getAsDouble();

                        CPInfo info = cps.computeIfAbsent(cpID, _ -> new CPInfo());
                        synchronized (info) {
                            info.cpID = cpID;
                            info.ubicacion = loc;
                            info.precio = price;
                            info.estado = "ACTIVADO";
                            info.lastHb = System.currentTimeMillis();
                        }

                    }
                    // -------- Monitor: heartbeat --------
                    case "HB" -> {
                        String cpID = msg.get("cp").getAsString();
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
        } finally {}
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
            if (os.contains("win")) {                       // Windows
            new ProcessBuilder("cmd", "/c", "cls")
                .inheritIO()                               // usa la misma TTY
                .start().waitFor();
            } 
            else {                                        // Linux / macOS
                System.out.print("\033[H\033[2J");
                System.out.flush();
            }
        } catch (Exception e) {
            // Plan C: si todo falla, “desplaza” la pantalla
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
                        String cpID = e.getKey();
                        CPInfo info = e.getValue();
                        String sID = cpSesionesActivas.get(cpID);

                        long lag = now - info.lastHb;
                        if (lag > 3000) { // 3s sin latidos -> desconectado
                            boolean cortar = false;
                            synchronized (info) {
                                if (!"DESCONECTADO".equals(info.estado)) {
                                    info.estado = "DESCONECTADO";
                                    cortar = info.ocupado; // si estaba suministrando, cortamos
                                }
                            }
                            if (cortar && sID != null) {
                                stopSolicitado.add(sID);
                                bus.publish(T_CMD, cpID, obj("type","CMD","ts",System.currentTimeMillis(),"cmd","STOP_SUPPLY","cp",cpID));
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
            String json = buildStatusJson(); // ver abajo
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
            String sesion = cpSesionesActivas.getOrDefault(cpID, "-");
            double kwh=0, eur=0;
            var sInf = sesiones.get(sesion);
            if (sInf != null) { 
                kwh = sInf.kWhAccumulado; eur = sInf.eurAccumulado;
            }
            String est = estadoVisible(info);
            long lag = now - info.lastHb;

            if (!first) sb.append(',');
            first = false;
            sb.append("{")
            .append("\"cp\":\"").append(cpID).append("\",")
            .append("\"estado\":\"").append(est).append("\",")
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

    private void handleCmd(com.sun.net.httpserver.HttpExchange ex) { // GET /cmd?op=PAUSE&cp=CP-001   |  RESUME  |  STOP
        try {
            var q = ex.getRequestURI().getQuery(); // op=...&cp=...
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
        CPInfo info = cps.get(cpID);
        if (info != null) { synchronized (info) { info.parado = true; } }
        String sID = cpSesionesActivas.get(cpID);
        if (sID!=null) stopSolicitado.add(sID);
        bus.publish(T_CMD, cpID, obj("type","CMD","ts",System.currentTimeMillis(),"cmd","STOP_SUPPLY","cp",cpID));
        return "ACK PAUSE " + cpID + " (STOP_SUPPLY publicado)";
    }

    private String resumeCp(String cpID){
        CPInfo info = cps.get(cpID);
        if (info==null) return "ERR CP_DESCONOCIDO";
        synchronized(info){ 
            info.parado = false; 
            if (!"AVERIADO".equals(info.estado)) info.estado="ACTIVADO"; 
        }
        bus.publish(T_CMD, cpID, obj("type","CMD","ts",System.currentTimeMillis(),"cmd","RESUME","cp",cpID));
        return "ACK RESUME " + cpID + " (RESUME publicado)";
    }

    private String stopCp(String cpID){
        String sId = cpSesionesActivas.get(cpID);
        if (sId!=null) stopSolicitado.add(sId);
        bus.publish(T_CMD, cpID, obj("type","CMD","ts",System.currentTimeMillis(),"cmd","STOP_SUPPLY","cp",cpID));
        return "ACK STOP " + cpID + " (STOP_SUPPLY publicado)";
    }
    
    private void onKafkaCmd(com.google.gson.JsonObject m) {
        System.out.println("[CENTRAL][KAFKA] RX CMD: " + m);
        try {
            if (!m.has("type") || !"CMD".equals(m.get("type").getAsString())) return;
            String cmd = m.get("cmd").getAsString();

            switch (cmd) {
                case "STOP_SUPPLY" -> System.out.println("[CENTRAL][KAFKA] " + stopCp(m.get("cp").getAsString()));
                case "PAUSE"       -> System.out.println("[CENTRAL][KAFKA] " + pauseCp(m.get("cp").getAsString()));
                case "RESUME"      -> System.out.println("[CENTRAL][KAFKA] " + resumeCp(m.get("cp").getAsString()));

                // NUEVO: DRIVER -> CENTRAL por Kafka
                case "REQ_START" -> {
                    String driverId = m.get("driver").getAsString();
                    String cpID     = m.get("cp").getAsString();

                    if (!ensureDriver(driverId)) {
                        bus.publish(T_SESSIONS, driverId,
                            obj("type","AUTH","ts",System.currentTimeMillis(),
                                "ok",false,"driver",driverId,"cp",cpID,"reason","DRIVER_INVALIDO"));
                        return;
                    }
                    CPInfo info = cps.get(cpID);
                    if (info == null) {
                        bus.publish(T_SESSIONS, driverId,
                            obj("type","AUTH","ts",System.currentTimeMillis(),
                                "ok",false,"driver",driverId,"cp",cpID,"reason","CP_DESCONOCIDO"));
                        return;
                    }

                    synchronized (info) {
                        if (!"ACTIVADO".equals(info.estado)) {
                            bus.publish(T_SESSIONS, driverId,
                                obj("type","AUTH","ts",System.currentTimeMillis(),
                                    "ok",false,"driver",driverId,"cp",cpID,"reason",info.estado));
                            return;
                        }
                        if (info.parado) {
                            bus.publish(T_SESSIONS, driverId,
                                obj("type","AUTH","ts",System.currentTimeMillis(),
                                    "ok",false,"driver",driverId,"cp",cpID,"reason","PARADO"));
                            return;
                        }
                        if (info.ocupado) {
                            bus.publish(T_SESSIONS, driverId,
                                obj("type","AUTH","ts",System.currentTimeMillis(),
                                    "ok",false,"driver",driverId,"cp",cpID,"reason","OCUPADO"));
                            return;
                        }

                        // Crear sesión
                        String sesId = "S-" + System.currentTimeMillis();
                        info.ocupado = true;
                        var sInf = new SesionInfo(sesId, cpID, driverId);
                        sesiones.put(sesId, sInf);
                        cpSesionesActivas.put(cpID, sesId);

                        // Orden al Engine por Kafka
                        bus.publish(T_CMD, cpID, obj("type","CMD","ts",System.currentTimeMillis(),
                                                    "cmd","START_SUPPLY","session",sesId,"cp",cpID,"price",info.precio));

                        // DB
                        try { dbOpenSession(sesId, cpID, driverId, info.precio); } catch (Exception ignore) {}

                        // Noticia general para terceros (monitoring, etc.)
                        bus.publish(T_SESSIONS, sesId, obj("type","SESSION_START","ts",System.currentTimeMillis(),
                                                        "session",sesId,"cp",cpID,"driver",driverId,"price",info.precio));

                        // *** IMPORTANTE: Respuesta directa al Driver (lo que te faltaba) ***
                        // ...dentro del synchronized(info) y después de publicar START_SUPPLY y SESSION_START:
                        bus.publish(T_SESSIONS, driverId,
                            obj("type","AUTH","ts",System.currentTimeMillis(),
                                "ok", true,
                                "driver", driverId,
                                "cp", cpID,
                                "session", sesId,
                                "price", info.precio));

                    }
                }

            }
        } catch (Exception e) {
            System.err.println("[CENTRAL][KAFKA] onCmd: " + e.getMessage());
        }
    }

    private void onKafkaTelemetry(com.google.gson.JsonObject m) {
        try {
            if (!m.has("type") || !"TEL".equals(m.get("type").getAsString())) return;
            String ses = m.get("session").getAsString();
            double kwh = m.get("kwh").getAsDouble();
            double eur = m.get("eur").getAsDouble();
            var sInf = sesiones.get(ses);
            if (sInf != null) { sInf.kWhAccumulado = kwh; sInf.eurAccumulado = eur; }
        } catch (Exception e) {
            System.err.println("[CENTRAL][KAFKA] TEL: " + e.getMessage());
        }
    }

    private void onKafkaSessions(com.google.gson.JsonObject m) {
        try {
            if (!m.has("type")) return;
            String t = m.get("type").getAsString();
            if (!"SESSION_END".equals(t)) return;

            String sesId = m.get("session").getAsString();
            String cpID  = m.get("cp").getAsString();
            double kwh   = m.has("kwh") ? m.get("kwh").getAsDouble() : 0.0;
            double eur   = m.has("eur") ? m.get("eur").getAsDouble() : 0.0;
            String reason= m.has("reason") ? m.get("reason").getAsString() : "OK";

            sesiones.remove(sesId);
            cpSesionesActivas.remove(cpID, sesId);

            var info = cps.get(cpID);
            if (info != null) synchronized (info) {
                info.ocupado = false;
                if (!info.parado && !"AVERIADO".equals(info.estado)) info.estado = "ACTIVADO";
            }

            try { dbCloseSession(sesId, reason, kwh, eur); } catch (Exception ignore) {}

        } catch (Exception e) {
            System.err.println("[CENTRAL][KAFKA] SESSION_END: " + e.getMessage());
        }
    }
}

