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

public class CPEngine {

    private static volatile boolean enchufado = false;   // PLUG/UNPLUG
    private static volatile boolean enMarcha  = false;   // suministrando
    private static volatile boolean healthy   = true;    // para el monitor (KO/OK)

    private static volatile String  sesionActiva = null;
    private static volatile String  cpIDActual   = null;

    private static volatile boolean stopByCmd = false;

    // Simulación (ajusta a tus nombres/valores)
    private static volatile double potenciaKW = 7.20;

    private static int parseIntOr(String s, int def) {
        try {
            return Integer.parseInt(s);
        }
        catch (Exception e) {
            return def;
        }
    }
    
   private static double parseDoubleOr(String s, double def) {
        try { return Double.parseDouble(s); }
        catch (Exception e) { return def; }
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
                                }
                                else{
                                    out.writeUTF("UNKNOWN");
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

    private static void iniciarConsola() {
        
        Thread consola = new Thread(() -> {
            try (var br = new java.io.BufferedReader(new java.io.InputStreamReader(System.in))){
                System.out.println("[ENG] Comandos: PLUG | UNPLUG | STATUS | OK | KO");
                for (String line; (line = br.readLine()) !=null; ){
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
                            System.out.println("[ENG] enchufado=" + enchufado +
                                " enMarcha=" + enMarcha +
                                " sesion=" + sesionActiva +
                                " cp=" + cpIDActual +
                                " healthy=" + healthy +
                                " pKW=" + potenciaKW);
                        }
                        case "OK" -> {
                            healthy = true;
                            System.out.println("[ENG] Salud = OK");
                        }
                        case "KO" -> {
                            healthy = false;
                            System.out.println("[ENG] Salud = KO");
                        }
                    
                        default -> {
                            if (!line.isBlank()) {
                                System.out.println("[ENG] Comando desconocido");
                            }
                        }
                    }
                }
            }
            catch (Exception ignore){}
        }, "engine-console");
        consola.setDaemon(true);
        consola.start();
    }

    private static void conectarYAtenderCentral(String host, int port, String cpID) {
        while (true) {
            try (var s   = new java.net.Socket(host, port);
                var in  = new java.io.DataInputStream(s.getInputStream());
                var out = new java.io.DataOutputStream(s.getOutputStream())) {

                System.out.println("[ENG] Conectado a CENTRAL " + host + ":" + port);

                // 1) Vincular este canal al CP (ENGINE_BIND)
                send(out, obj("type","ENGINE_BIND","ts",System.currentTimeMillis(),"cp",cpID));

                // 2) Escuchar órdenes
                while (true) {
                    JsonObject m = recv(in);
                    String type = m.get("type").getAsString();

                    if ("START".equals(type)) {
                        String session = m.get("session").getAsString();
                        String cp      = m.get("cp").getAsString();
                        double price   = m.get("price").getAsDouble();

                        
                        sesionActiva = session; cpIDActual = cp; enMarcha = true;
                        System.out.println("[ENG] START sesión " + session + " en " + cp + " (precio " + price + ")");
                        
                        // Esperar enchufe (o STOP) hasta 10s antes de empezar a telemetrear
                        long t0Plug = System.currentTimeMillis();
                        while (enMarcha && !enchufado && session.equals(sesionActiva)) {
                            if (System.currentTimeMillis() - t0Plug > 10_000) break; // timeout opcional
                            Thread.sleep(50);
                        }

                        // Bucle de telemetrías (1s)
                        double kwh = 0.0, eur = 0.0;
                        long last = System.currentTimeMillis();

                        while (enMarcha && enchufado && session.equals(sesionActiva)) {
                            long now = System.currentTimeMillis();
                            if (now - last < 1000) { Thread.sleep(10); continue; }
                            last = now;

                            // Simulación sencilla
                            kwh += potenciaKW / 3600.0;
                            eur  = kwh * price;

                            send(out, obj(
                                "type","TEL","ts",now,
                                "session",session,"cp",cp,
                                "pkw",potenciaKW,"kwh",kwh,"eur",eur
                            ));
                        }

                        // Fin de sesión
                        String reason;
                        if (stopByCmd)      reason = "STOP";
                        else if (!healthy)  reason = "KO";   // opcional: reflejar avería local
                        else                reason = "OK";

                        send(out, obj(
                            "type","END","ts",System.currentTimeMillis(),
                            "session",session,"cp",cp,"reason",reason
                        ));
                        System.out.println("[ENG] END sesión " + session + " (" + reason + ")");

                        // Reset flags
                        stopByCmd = false;
                        enMarcha = false; sesionActiva = null; cpIDActual = null;

                    }
                    else if ("CMD".equals(type)) {
                        String cmd = m.get("cmd").getAsString();
                        String cp  = m.has("cp") ? m.get("cp").getAsString() : null;
                        if ("STOP_SUPPLY".equals(cmd)) {
                            System.out.println("[ENG] CMD STOP_SUPPLY" + (cp!=null?(" "+cp):""));
                            stopByCmd = true;
                            enMarcha = false; // el bucle de TEL saldrá
                        } else {
                            System.out.println("[ENG] CMD desconocido: " + cmd);
                        }
                    }
                }

            } catch (Exception e) {
                System.out.println("[ENG] Desconectado de CENTRAL: " + e.getMessage() + " (reintento 1s)");
                try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
            }
        }
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
        String host = config.getProperty("engine.centralHost", "127.0.0.1");
        int puerto = parseIntOr(config.getProperty("engine.centralPort","5000"),5000);
        int puertoHealth = parseIntOr(config.getProperty("engine.healthPort","6100"),6100);
        String cpID = config.getProperty("engine.cpId", "CP-001");
        potenciaKW = parseDoubleOr(config.getProperty("engine.potenciaKW","7.2"),7.2);


        iniciarHealthServer(puertoHealth);
        iniciarConsola();
        conectarYAtenderCentral(host, puerto, cpID);
        
    }

 
}
