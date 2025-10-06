package cp_monitor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Properties;

public class CPMonitor {

    private static int parseIntOr(String s, int def) {
        try {
            return Integer.parseInt(s);
        }
        catch (Exception e) {
            return def;
        }
    }

    public static void main(String[] args) throws Exception {
        String rutaConfig = "config/monitor.config";

        Properties config = new Properties();
        Path ruta = Path.of(rutaConfig).toAbsolutePath().normalize();

        try (InputStream fichero = Files.newInputStream(ruta)) {
            config.load(fichero);
        } catch (NoSuchFileException e) {
            System.err.println("[MON] No encuentro el fichero de configuración: " + ruta);
            System.exit(1);
            return;
        } catch (Exception e) {
            System.err.println("[MON] Error leyendo configuración en " + ruta + ": " + e.getMessage());
            System.exit(1);
            return;
        }

        String centralHost   = config.getProperty("monitor.centralHost", "127.0.0.1");
        int    centralPort   = parseIntOr(config.getProperty("monitor.centralPort"), 5000);
        String engineHost   = config.getProperty("monitor.engineHost", "127.0.0.1");
        int    enginePort   = parseIntOr(config.getProperty("monitor.enginePort"), 6100);
        String cpID   = config.getProperty("monitor.cpId", "CP-001");
        String ubicacion   = config.getProperty("monitor.ubicacion", "Valencia-Centro");
        String precio = config.getProperty("monitor.precio", "0.35");
        

        try (var sC = new Socket(centralHost, centralPort);
             var inC = new DataInputStream(sC.getInputStream());
             var outC = new DataOutputStream(sC.getOutputStream())) {

            // REG_CP SIEMPRE aunque Engine esté apagado
            String reg = "REG_CP " + cpID + " " + ubicacion + " " + precio;
            outC.writeUTF(reg);
            try { 
                System.out.println("[MON] <- " + inC.readUTF()); 
            } 
            catch (Exception ignore){}

            Socket sE = null;
            DataInputStream inE = null;
            DataOutputStream outE = null;

            while (true) {
                // Asegurar conexión al Engine (si no hay, probar a conectar)
                if (sE == null || sE.isClosed()) {
                    try {
                        sE = new Socket(engineHost, enginePort);
                        inE  = new DataInputStream(sE.getInputStream());
                        outE = new DataOutputStream(sE.getOutputStream());
                        System.out.println("[MON] Conectado a Engine " + engineHost + ":" + enginePort);
                    } catch (Exception ex) {
                        sE = null; inE = null; outE = null;
                    }
                }

                String estado = "OK";
                try {
                    if (outE == null) throw new IllegalStateException("no-engine");
                    outE.writeUTF("PING");
                    String r = inE.readUTF();
                    if (!"OK".equalsIgnoreCase(r)) estado = "KO";
                } catch (Exception ex) {
                    estado = "KO";
                    // Si hubo error, cierra y reintenta en el próximo ciclo
                    try { if (sE != null) sE.close(); } catch (Exception ignore) {}
                    sE = null; inE = null; outE = null;
                }

                // Enviar HB a Central SIEMPRE
                outC.writeUTF("HB " + cpID + " " + estado);
                try { System.out.println("[MON] <- " + inC.readUTF()); } catch (Exception ignore){}

                Thread.sleep(1000);
            }       
        }
    }
}
