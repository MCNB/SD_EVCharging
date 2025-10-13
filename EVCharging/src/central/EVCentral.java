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
    private final Map<String, DataOutputStream> engineOut = new ConcurrentHashMap<>();
    private final Map<String, SesionInfo> sesiones = new ConcurrentHashMap<>();
    private final Map<String, String> cpSesionesActivas = new ConcurrentHashMap<>();
    private final Map<String, DataOutputStream> sesionToDriver = new ConcurrentHashMap<>();
    private final java.util.Set<String> stopSolicitado = java.util.concurrent.ConcurrentHashMap.newKeySet(); //Para ver si el END viene de un STOP manual
    private final static java.util.Set<String> driversValidos = java.util.concurrent.ConcurrentHashMap.newKeySet();
    

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
        } catch (Exception e) { System.out.println("[CENTRAL][DB] spOpenSession " + e.getMessage()); }
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
        } catch (Exception e) { System.out.println("[CENTRAL][DB] spCloseSession " + e.getMessage()); }
    }

    private String pauseCp(String cpID){
        CPInfo info = cps.get(cpID);
        if (info==null) return "ERR CP_DESCONOCIDO";
        synchronized(info){ 
            info.parado = true; 
        }
        // si hay sesión, ordena corte
        String sID = cpSesionesActivas.get(cpID);
        var eng = engineOut.get(cpID);
        if (sID!=null && eng!=null) {
            stopSolicitado.add(sID);
            try { 
                eng.writeUTF("STOP_SUPPLY " + cpID); 
                eng.flush(); 
            } 
            catch (Exception e){ 
                engineOut.remove(cpID, eng); 
            }
        }
        return "ACK PAUSE " + cpID;
    }

    private String resumeCp(String cpID){
        CPInfo info = cps.get(cpID);
        if (info==null) return "ERR CP_DESCONOCIDO";
        synchronized(info){ 
            info.parado = false; 
            if (!"AVERIADO".equals(info.estado)) info.estado="ACTIVADO"; 
        }
        return "ACK RESUME " + cpID;
    }

    private String stopCp(String cpID){
        String sID = cpSesionesActivas.get(cpID);
        var eng = engineOut.get(cpID);
        if (sID==null) return "ACK STOP " + cpID + " NO_SESSION";
        if (eng==null)  return "ACK STOP " + cpID + " NO_ENGINE";
        stopSolicitado.add(sID);
        try { 
            eng.writeUTF("STOP_SUPPLY " + cpID); 
            eng.flush(); 
            return "ACK STOP " + cpID + " SENT"; 
        }
        catch(Exception e){ 
            engineOut.remove(cpID, eng); 
            return "ACK STOP " + cpID + " ENGINE_IO_ERROR"; 
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

    private void atender (Socket s) {
        String cliente = s.getRemoteSocketAddress().toString();
        System.out.println("[CENTRAL] Conexión " + cliente);
        //Al salir del bloque cuando el cliente se cierra o caza una excepción, java cierra en orden inverso: out->in->s
        try (s;
             var in = new DataInputStream(s.getInputStream());
             var out = new DataOutputStream(s.getOutputStream())) {
            while (true) {
                String msg = in.readUTF();
                String[] p = msg.trim().split("\s+");
                if (p.length == 0) continue;
                String comando = p[0].toUpperCase();

                switch (comando) {
                    //Recibe alta del punto de carga - Recibido de Monitor - REG_CP <cpID> <ubicacion_sin_espacios> <precio>
                    case "REG_CP" -> {
                        if (p.length < 4) {
                            out.writeUTF("ERR Formato REG_CP");
                            continue;
                        }
                        String cpID = p[1], ubi = p[2];
                        double precio = Double.parseDouble(p[3]);
                        //"Crear si no está" en mapa concurrente
                        //Si existe un valor de cpID como key, devuelve la info asociada
                        //Si no, mediante una función lambda, crea el valor del constructor por defecto para ese cpID y lo devuelve
                        /*
                            CPInfo info = cps.get(cpID);
                            if (info == null) {
                              CPInfo nuevo = new CPInfo();
                              CPInfo anterior = cps.putIfAbsent(cpID, nuevo);
                              info = (anterior == null) ? nuevo : anterior;
                            }
                         */
                        CPInfo info = cps.computeIfAbsent(cpID, _ -> new CPInfo());
                        info.cpID = cpID; info.ubicacion = ubi; info.precio = precio; info.estado = "ACTIVADO";
                        System.out.println("[CENTRAL] Registrado CP " + cpID + " @ " + ubi + " precio " + precio);
                        out.writeUTF("ACK REG_CP " + cpID);
                    }
                    //Recibe un HeartBeat - Recibido de Monitor - ACK REG_CP <cpID>
                    case "HB" -> {
                        if (p.length < 3) {
                            out.writeUTF("ERR Formato HB");
                            continue;
                        }
                        String cpID = p[1]; 
                        String estado = p[2].toUpperCase();

                        CPInfo info = cps.get(cpID);
                        if (info==null) {
                            out.writeUTF("ERR CP_DESCONOCIDO");
                            continue;
                        }

                        info.lastHb = System.currentTimeMillis();

                        if ("KO".equals(estado)) {
                            synchronized (info) {
                                info.estado = "AVERIADO";
                            }
                            String sID = cpSesionesActivas.get(cpID);
                            DataOutputStream eng = engineOut.get(cpID);
                            if (sID != null && eng != null) {
                                stopSolicitado.add(sID);
                                try {
                                    eng.writeUTF("STOP_SUPPLY " + cpID);
                                    eng.flush();
                                }
                                catch (Exception e){
                                    engineOut.remove(cpID, eng);
                                }
                            }
                        }
                        else {
                            synchronized (info){
                                if (!info.parado) {
                                    info.estado = "ACTIVADO";
                                }
                            }
                        }
                        out.writeUTF("ACK HB " + cpID + " " + estado);
                    }
                    //Recibe orden de suministro - Recibido de Driver - REQ_START <driverID> <cpID>
                    case "REQ_START" -> {
                        if (p.length < 3) {
                            out.writeUTF("AUTH_NO Formato ERR");
                            continue;
                        }
                        String driverID = p[1], cpID = p[2];
                        if (!ensureDriver(driverID)) {
                            out.writeUTF("AUTH_NO DRIVER_ID_INVALIDO");
                            break;
                        }
                        CPInfo info = cps.get(cpID);
                        if (info == null) {
                            out.writeUTF("AUTH_NO CP_DESCONOCIDO");
                            continue;
                        }
                        synchronized (info) {
                            if (!"ACTIVADO".equals(info.estado)) {
                                out.writeUTF("AUTH_NO " + info.estado);
                                break;
                            }
                            if (info.parado) {
                                out.writeUTF("AUTH_NO PARADO");
                                break;
                            }
                            if (info.ocupado) {
                                out.writeUTF("AUTH_NO OCUPADO");
                                break;
                            }

                            DataOutputStream eng = engineOut.get(cpID);
                            if (eng == null) {
                                out.writeUTF("AUTH_NO NO_ENGINE");
                                break;
                            }

                            info.ocupado = true;
                            String sesionID = "S-" + java.time.Instant.now().toEpochMilli();
                            double precio = info.precio;

                            SesionInfo sesionInfo = new SesionInfo(sesionID, cpID, driverID);
                            sesiones.put(sesionID, sesionInfo);
                            cpSesionesActivas.put(cpID, sesionID);


                            try {
                                String start = "START " + sesionID + " " + cpID + " " + precio;
                                eng.writeUTF(start);
                                eng.flush(); //Se envía rápido para que el engine reciba el comando cuanto antes
                                System.out.println("[CENTRAL] -> ENGINE " + start);

                                dbOpenSession(sesionID, cpID, driverID, precio);
                            }
                            catch (Exception ex) {
                                info.ocupado = false;
                                sesiones.remove(sesionID);
                                cpSesionesActivas.remove(cpID, sesionID);
                                engineOut.remove(cpID, eng); //el 'out' de ese Engine ya no es fiable
                                out.writeUTF("AUTH_NO ENGINE_IO_ERROR");
                                System.out.println("[CENTRAL] Error enviando START a Engine " + cpID + ": " + ex.getMessage());
                                break;
                            }

                            sesionToDriver.put(sesionID, out);// recuerda el canal de vuelta al driver para enviarle el ticket.
                            System.out.println("[CENTRAL] AUTH_OK driver " + sesionID + " " + cpID +  " " + precio);
                            out.writeUTF("AUTH_OK " + sesionID + " " + cpID +  " " + precio);

                        }
                    }

                    case "ENGINE_BIND" ->{ // ENGINE_BIND <cpID>
                        if (p.length < 2) {
                            out.writeUTF("ENGINE_BIND Formato ERR");
                            continue;
                        }
                        String cpID = p[1];
                        if (!cps.containsKey(cpID)) {
                            out.writeUTF("ERR CP_NO_REGISTRADO");
                            continue;
                        }
                        engineOut.put(cpID, out); //Guardamos este "out" para enviar START/STOP a este Engine
                        out.writeUTF("ACK ENGINE_BIND " + cpID);
                        System.out.println("[CENTRAL] ENGINE_BIND " + cpID + " vinculado");

                    }

                    case "TEL" -> { // TEL <sesionID> <cpID> <powerKW> <kwhAcc> <eurAcc>
                        if (p.length < 6) {
                            out.writeUTF("ERR Formato TEL");
                            continue;
                        }
                        try {
                            String sesionID = p[1], cpID = p[2];
                            double kWhAcumulado = Double.parseDouble(p[4]);
                            double eurAcumulado = Double.parseDouble(p[5]);

                            var sesionInfo = sesiones.get(sesionID);
                            if(sesionInfo != null) {
                                sesionInfo.kWhAccumulado = kWhAcumulado;
                                sesionInfo.eurAccumulado = eurAcumulado;
                            }
                            System.out.printf("[CENTRAL] TEL %s %s kWh=%.5f eur=%.4f%n",
                                    sesionID, cpID, kWhAcumulado, eurAcumulado);
                        }
                        catch (Exception ex) {
                            System.out.println("[CENTRAL] TEL inválido: " + String.join(" ", p) + " :: " + ex.getMessage());
                            out.writeUTF("ERR TEL_FORMAT");
                            continue;
                        }
                    }

                    case "END" -> {
                        if (p.length < 3) {
                            out.writeUTF("ERR Formato END");
                            continue;
                        }
                        String sesionID = p[1], cpID = p[2];

                        var sesionInfo = sesiones.remove(sesionID);
                        cpSesionesActivas.remove(cpID, sesionID);

                        var info = cps.get(cpID);
                        if (info != null){
                            synchronized (info){
                                info.ocupado = false;
                            }
                        }
                        double kWh = (sesionInfo != null) ? sesionInfo.kWhAccumulado : 0.0;
                        double eur = (sesionInfo != null) ? sesionInfo.eurAccumulado : 0.0;
                        String motivo = stopSolicitado.remove(sesionID) ? "STOP" : "OK";

                        System.out.printf("[CENTRAL] END %s en %s -> CP libre (kWh=%.4f €=%.4f, %s)%n",
                                sesionID, cpID, kWh, eur, motivo);

                        String ticket = String.format(java.util.Locale.ROOT,"TICKET %s %s %.5f kWh %.4f eur %s", sesionID, cpID, kWh, eur, motivo);
                        var drv = sesionToDriver.remove(sesionID);
                        if (drv != null) {
                            try {
                                drv.writeUTF(ticket);
                            } 
                            catch (Exception ignore) {}
                        }

                        dbCloseSession(sesionID, motivo, kWh, eur);

                        out.writeUTF("ACK END " + sesionID + " " + cpID);
                    }
                    //Se recibe una orden de parada de un CP y lo cambia de OCUPADO a ACTIVADO (Libera el punto de carga)
                    //ACK STOP <cpID>
                    case "STOP" -> {
                        if (p.length < 2) {
                            out.writeUTF("ERR Formato STOP");
                            continue;
                        }
                        String cpID = p[1];

                        String sesionID = cpSesionesActivas.get(cpID);
                        if (sesionID == null) {
                            out.writeUTF("ACK STOP " + cpID + " NO_SESION");
                            break;
                        }

                        DataOutputStream eng = engineOut.get(cpID);
                        if (eng == null) {
                            out.writeUTF("ACK STOP " + cpID + " NO_SESION");
                        }

                        stopSolicitado.add(sesionID);
                        try {
                            String cmd = "STOP_SUPPLY " + cpID;
                            eng.writeUTF(cmd);
                            eng.flush();
                            System.out.println("[CENTRAL] -> ENGINE " + cmd + " (esperando END)");
                            out.writeUTF("ACK STOP " + cpID + " SENT");
                        }
                        catch (Exception ex){
                            engineOut.remove(cpID, eng);
                            out.writeUTF("ACK STOP " + cpID + " ENGINE_IO_ERROR");
                        }
                    }
                    case "PAUSE" -> {
                        if (p.length < 2) {
                            out.writeUTF("ERR Formato PAUSE");
                            continue;
                        }
                        String cpID = p[1];
                        CPInfo info = cps.get(cpID);
                        if (p.length < 2) {
                            out.writeUTF("ACK PAUSE " + cpID + " CP_DESCONOCIDO)");
                            break;
                        }

                        synchronized (info){
                            info.parado = true;
                        }
                        out.writeUTF("[CENTRAL] CP " + cpID + " marcado como PARADO (bloquea nuevas autorizaciones)");

                        String sesionID = cpSesionesActivas.get(cpID);
                        DataOutputStream eng = engineOut.get(cpID);
                        if (sesionID != null && eng != null) {
                            stopSolicitado.add(sesionID);
                            try {
                                eng.writeUTF("STOP_SUPPLY " + cpID);
                                eng.flush();
                            } catch (Exception e) {
                                engineOut.remove(cpID, eng);
                            }
                        }
                        out.writeUTF("ACK PAUSE " + cpID);
                    }

                    case "RESUME" -> {
                        if (p.length < 2) {
                            out.writeUTF("ERR Formato RESUME");
                            continue;
                        }
                        String cpID = p[1];
                        CPInfo info = cps.get(cpID);
                        if (p.length < 2) {
                            out.writeUTF("ACK RESUME " + cpID + " CP_DESCONOCIDO)");
                            break;
                        }
                        synchronized (info){
                            info.parado = false;
                            if ("AVERIADO".equals(info.estado)) {
                                //se puede dejar así y derar que el Monitor lo modifique con el HB
                            }
                        }
                        System.out.println("[CENTRAL] CP " + cpID + " REANUDADO (se permiten autorizaciones)");
                        out.writeUTF("ACK RESUME " + cpID);
                    }
                    
                    default -> out.writeUTF("ERR Comando no reconocido");
                }
            }
        } catch (Exception e) {
            System.out.println("[CENTRAL] Conexión cerrada (" + cliente + "): " + e.getMessage());
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
                    for (Map.Entry<String, CPInfo> e : cps.entrySet()) { // <- tipo explícito: cero problemas
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

                    long lag = now - info.lastHb;
                    if (lag > 3000) { // 3s sin latidos -> desconectado
                        boolean cortar = false;
                        synchronized (info) {
                            if (!"DESCONECTADO".equals(info.estado)) {
                                info.estado = "DESCONECTADO";
                                cortar = info.ocupado; // si estaba suministrando, cortamos
                            }
                        }
                        if (cortar) {
                            String sid = cpSesionesActivas.get(cpID);
                            DataOutputStream eng = engineOut.get(cpID);
                            if (sid != null && eng != null) {
                                stopSolicitado.add(sid);
                                try { eng.writeUTF("STOP_SUPPLY " + cpID); eng.flush(); }
                                catch (Exception ex) { engineOut.remove(cpID, eng); }
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
            var rs = st.executeQuery("SELECT cp_id, ubicacion, precio FROM dbo.CP")) {
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


    //Test para GIT

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
}
