package driver;

import com.google.gson.JsonObject;
import static common.net.Wire.*;

import java.io.*;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;

import common.bus.EventBus;
import common.bus.NoBus;
import common.bus.KafkaBus;

public class EVDriver {

    // ---- Helpers de lectura de config (sin args) ----
    private static Properties loadProps(String defaultPath) throws IOException {
        String ruta = (defaultPath != null && !defaultPath.isBlank()) ? defaultPath : "config/driver.config";
        Path p = Path.of(ruta).toAbsolutePath().normalize();
        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(p)) { props.load(in); }
        System.out.println("[DRV] Config: " + p);
        return props;
    }
    private static int getInt(Properties p, String k, int def){ try { return Integer.parseInt(p.getProperty(k)); } catch(Exception e){ return def; } }
    private static List<String> loadServicios(Path path) throws IOException {
        if (path == null) return List.of();
        List<String> out = new ArrayList<>();
        for (String line : Files.readAllLines(path, StandardCharsets.UTF_8)) {
            String s = line.strip();
            if (s.isEmpty() || s.startsWith("#") || s.startsWith("//")) continue;
            out.add(s.replace(' ', '-'));
        }
        return out;
    }
    private static EventBus bus = new NoBus();
    public static void main(String[] args) {
        try {
            // 1) Cargar configuración
            Properties cfg = loadProps("config/driver.config");
            String host     = cfg.getProperty("driver.centralHost", "127.0.0.1");
            int    port     = getInt(cfg, "driver.centralPort", 5000);
            String driverId = cfg.getProperty("driver.id", "D-001");

            String file = cfg.getProperty("driver.file", "").trim();
            final boolean modoFichero = !file.isEmpty();
            final List<String> cpList = modoFichero ? loadServicios(Path.of(file)) : List.of();

            System.out.printf("[DRV] Conectando a CENTRAL %s:%d como %s%n", host, port, driverId);
            if (modoFichero) System.out.println("[DRV] Modo fichero: " + file + " ("+cpList.size()+" servicios)");

            // 2) Bucle de reconexión simple
            while (true) {
                try (Socket s = new Socket(host, port);
                     DataInputStream  in  = new DataInputStream(s.getInputStream());
                     DataOutputStream out = new DataOutputStream(s.getOutputStream());
                     BufferedReader br = modoFichero ? null : new BufferedReader(new InputStreamReader(System.in))) {

                    System.out.println("[DRV] Conectado.");

                    if (modoFichero) {
                        ejecutarModoFichero(driverId, cpList, in, out);
                        System.out.println("[DRV] Fichero procesado. Fin.");
                        return; // terminar tras procesar el fichero
                    } else {
                        ejecutarModoManual(driverId, in, out, br);
                        System.out.println("[DRV] Fin (manual).");
                        return;
                    }

                } catch (Exception e) {
                    System.out.println("[DRV] Desconectado de CENTRAL: " + e.getMessage() + " (reintento 1s)");
                    try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
                }
            }

        } catch (Exception e) {
            System.err.println("[DRV] ERROR: " + e.getMessage());
            e.printStackTrace(System.err);
        }
    }

    // ----------------- MODO MANUAL -----------------
    private static void ejecutarModoManual(String driverId,
                                           DataInputStream in,
                                           DataOutputStream out,
                                           BufferedReader br) throws Exception {
        System.out.println("[DRV] Modo manual. Introduce CP-ID (Enter vacío para salir).");

        while (true) {
            System.out.print("CP-ID> ");
            String cp = br.readLine();
            if (cp == null) break;
            cp = cp.trim();
            if (cp.isEmpty()) break;
            String low = cp.toLowerCase(Locale.ROOT);
            if (low.equals("q") || low.equals("quit") || low.equals("exit")) break;

            // REQ_START
            // EVDriver (modo Kafka)
            bus.publish("ev.cmd.v1", cp, obj("type","CMD","cmd","REQ_START","ts",System.currentTimeMillis(),"driver",driverId,"cp",cp));

            send(out, obj("type","REQ_START","ts",System.currentTimeMillis(),"driver",driverId,"cp",cp));
            // AUTH
            JsonObject ans = recv(in);
            if (!"AUTH".equals(ans.get("type").getAsString())) {
                System.out.println("[DRV] Respuesta inesperada: " + ans);
                continue;
            }
            boolean ok = ans.get("ok").getAsBoolean();
            if (!ok) {
                String reason = ans.has("reason") ? ans.get("reason").getAsString() : "?";
                System.out.println("[DRV] DENEGADO: " + reason);
                continue;
            }
            String ses = ans.get("session").getAsString();
            String cpR = ans.get("cp").getAsString();
            double pr  = ans.get("price").getAsDouble();
            System.out.println("[DRV] AUTORIZADO: sesión " + ses + " en " + cpR + " (precio " + pr + ")");

            // Esperar TICKET
            while (true) {
                JsonObject ev = recv(in);
                String t = ev.get("type").getAsString();
                if ("TICKET".equals(t)) {
                    String sid = ev.get("session").getAsString();
                    String cpx = ev.get("cp").getAsString();
                    double kwh = ev.get("kwh").getAsDouble();
                    double eur = ev.get("eur").getAsDouble();
                    String rsn = ev.get("reason").getAsString();
                    System.out.printf("[DRV] 🎫 TICKET %s %s kWh=%.5f €=%.4f reason=%s%n", sid, cpx, kwh, eur, rsn);
                    break; // volver a pedir CP-ID
                } else {
                    System.out.println("[DRV] (evento) " + ev);
                }
            }
        }
    }

    // ----------------- MODO FICHERO -----------------
    private static void ejecutarModoFichero(String driverId,
                                            List<String> cpList,
                                            DataInputStream in,
                                            DataOutputStream out) throws Exception {
       
        for (int i = 0; i < cpList.size(); i++) {
            String cp = cpList.get(i);
            boolean realizado = false;
            int reintentos = 0;

            while (!realizado) {
                try {
                    // REQ_START
                    send(out, obj("type","REQ_START","ts",System.currentTimeMillis(),"driver",driverId,"cp",cp));
                    System.out.printf("[DRV] (%d/%d) -> REQ_START %s %s%n", i+1, cpList.size(), driverId, cp);

                    // AUTH
                    JsonObject ans = recv(in);
                    if (!"AUTH".equals(ans.get("type").getAsString())) {
                        System.out.println("[DRV] Respuesta inesperada: " + ans);
                        break; // pasa al siguiente CP
                    }
                    if (!ans.get("ok").getAsBoolean()) {
                        String reason = ans.has("reason") ? ans.get("reason").getAsString() : "?";
                        System.out.println("[DRV] DENEGADO: " + reason);
                        break; // pasa al siguiente CP
                    }

                    String ses = ans.get("session").getAsString();
                    String cpR = ans.get("cp").getAsString();
                    double pr  = ans.get("price").getAsDouble();
                    System.out.println("[DRV] AUTORIZADO: sesión " + ses + " en " + cpR + " (precio " + pr + ")");

                    // Esperar TICKET
                    while (true) {
                        JsonObject ev = recv(in);
                        if ("TICKET".equals(ev.get("type").getAsString())) {
                            String sid = ev.get("session").getAsString();
                            String cpx = ev.get("cp").getAsString();
                            double kwh = ev.get("kwh").getAsDouble();
                            double eur = ev.get("eur").getAsDouble();
                            String rsn = ev.get("reason").getAsString();
                            System.out.printf("[DRV] TICKET %s %s kWh=%.5f €=%.4f reason=%s%n", sid, cpx, kwh, eur, rsn);
                            realizado = true;
                            break;
                        } else {
                            System.out.println("[DRV] (evento) " + ev);
                        }
                    }

                } catch (Exception ex) {
                    reintentos++;
                    System.out.println("[DRV] Conexión caída durante el servicio ("+cp+"): " + ex.getMessage());
                    if (reintentos > 3) {
                        System.out.println("[DRV] Saltando "+cp+" tras 3 reintentos.");
                        break; // pasa al siguiente CP
                    }
                    System.out.println("[DRV] Reintentando "+cp+" en 1s...");
                    try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
                }
            }

            if (i < cpList.size() - 1) {
                System.out.println("[DRV] Esperando 4 segundos antes del siguiente servicio...");
                Thread.sleep(4000);
            }
        }
    }
}

