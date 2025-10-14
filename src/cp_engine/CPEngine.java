package cp_engine;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Properties;

public class CPEngine {

    static volatile boolean enchufado = false;
    static volatile boolean enMarcha = false;
    static volatile String sesionActiva = null;
    static volatile String cpIDActual = null;
    static volatile boolean healthy = true; //KO/OK

    static double potenciaKW;
    static int duracionDemoSec;

    private static int parseIntOr(String s, int def) {
        try {
            return Integer.parseInt(s);
        }
        catch (Exception e) {
            return def;
        }
    }
    private static Double parseDoubleOr(String s, double def) {
        try {
            return Double.parseDouble(s);
        }
        catch (Exception e) {
            return def;
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
        duracionDemoSec = parseIntOr(config.getProperty("engine.durationSec", "15"),15);
        double precio = parseDoubleOr(config.getProperty("engine.precio","0.35"),0.35);

        iniciarHealthServer(puertoHealth);
        iniciarConsola();
    

        while (true) {
            try (Socket s = new Socket(host, puerto);
             var in = new DataInputStream(s.getInputStream());
             var out = new DataOutputStream(s.getOutputStream())) {

                out.writeUTF("ENGINE_BIND " + cpID);
                //String ack = in.readUTF();

                while (true){
                    String msg = in.readUTF();
                    if (msg == null || msg.isBlank()) {
                        continue;
                    }
                    String[] p = msg.trim().split("\\s+");
                    String comando = p[0].toUpperCase();
                    

                    switch (comando){
                        case "START" -> {
                            
                            String sesionID = p[1], cp = p[2];
                            double precioPorkWh = parseDoubleOr(p[3], precio);
                            sesionActiva = sesionID;
                            cpIDActual = cp;

                            System.out.println("[ENG] START sesión " + sesionID + " en " + cp + " precio = " + precioPorkWh);
                            System.out.println("[ENG] Esperando PLUG...");
                            while (!enchufado && sesionID.equals(sesionActiva)) {
                                Thread.sleep(100);
                            }

                            if (!sesionID.equals(sesionActiva)) {
                                break;
                            }

                            enMarcha = true;
                            double kWh = 0.0, eur =0.0;
                            int seg = 0;
                            long t0 = System.currentTimeMillis();
                            while (enMarcha && enchufado && sesionID.equals(sesionActiva)) {
                                long t = System.currentTimeMillis();
                                if (t - t0 < 1000) {
                                    Thread.sleep(10);
                                    continue;
                                }
                                t0 = t;
                                kWh += potenciaKW / 3600.0;
                                eur = kWh * precioPorkWh;
                                String telemetria = String.format(Locale.ROOT,"TEL %s %s %.2f %.5f %.4f",
                                        sesionID, cp, potenciaKW, kWh, eur);
                                out.writeUTF(telemetria);
                                if (duracionDemoSec > 0 && ++seg >= duracionDemoSec) {
                                    enMarcha = false;
                                }
                               
                            }
                            out.writeUTF("END " + sesionID + " " + cp);
                            System.out.println("[ENG] -> END " + sesionID + " " + cp);
                            enMarcha = false;
                            sesionActiva = null;
                            cpIDActual = null;
                        }
                        
                        case "STOP_SUPPLY" -> {
                            System.out.println("[ENG] STOP_SUPPLY recibido");
                            enMarcha = false;
                        }
                        case "ACK" -> {//Ignora ACK
                        }

                        default -> System.out.println("[ENG] Comando desconocido: " + msg);
                    }
                }
            }
            catch (Exception e) {
                System.out.println("[ENG] Reconectando en 1s: " + e.getMessage());
                Thread.sleep(1000);
            }
        }      
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
                System.out.println("[ENG] Comandos: PLUG | UNPLUG | STATUS");
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
                            System.out.println("[ENG] Enchufado = " + enchufado + " En marcha = " + enMarcha + "Sesión = " + sesionActiva);
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
}
