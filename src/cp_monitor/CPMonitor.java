package cp_monitor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.nio.file.Files;

import java.nio.file.Path;
import java.util.Properties;

import static common.net.Wire.*;

public class CPMonitor {

    private static int getInt(Properties p, String k, int def){
        try { return Integer.parseInt(p.getProperty(k)); } catch(Exception e){ return def; }
    }
    private static double getDouble(Properties p, String k, double def){
        try { return Double.parseDouble(p.getProperty(k)); } catch(Exception e){ return def; }
    }
    public static void main(String[] args) throws Exception {
    // carga props
        String rutaConfig = "config/monitor.config";
        Properties p = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(rutaConfig))) { p.load(in); }

        final String centralHost = p.getProperty("monitor.centralHost", "127.0.0.1");
        final int centralPort    = getInt(p, "monitor.centralPort", 5000);
        final String cpId        = p.getProperty("monitor.cpId", "CP-001");
        final String ubic        = p.getProperty("monitor.ubicacion", "N/A");
        final double precio      = getDouble(p, "monitor.precio", 0.35);

        final String engineHost  = p.getProperty("monitor.engineHost", "127.0.0.1");
        final int enginePort     = getInt(p, "monitor.enginePort", 6100);

        System.out.printf("[MON] cfg central=%s:%d cp=%s ubic=%s precio=%.2f engine=%s:%d%n",
                centralHost, centralPort, cpId, ubic, precio, engineHost, enginePort);

        // bucle de reconexión al CENTRAL
        for (;;) {
            try (Socket sC = new Socket(centralHost, centralPort);
                DataInputStream  inC  = new DataInputStream(sC.getInputStream());
                DataOutputStream outC = new DataOutputStream(sC.getOutputStream())) {

                System.out.println("[MON] Conectado a CENTRAL " + centralHost + ":" + centralPort);

                // REG_CP (JSON vía Wire)
                send(outC, obj(
                    "type","REG_CP","ts",System.currentTimeMillis(),
                    "cp",cpId,"loc",ubic,"price",precio
                ));
                // Si Central responde algo, puedes leerlo; no es necesario
                 try { var ack = recv(inC); System.out.println("[MON] <- "+ack); } catch (Exception ignore){}

                // Mantén (o reintenta) conexión al health server del Engine
                

                while (true) {
                    // Asegura health socket (Engine)
                    boolean ok;
                    try (Socket sEngine = new Socket(engineHost, enginePort);
                        DataInputStream  inEngine  = new DataInputStream(sEngine.getInputStream());
                        DataOutputStream outEngine = new DataOutputStream(sEngine.getOutputStream())) {
                        outEngine.writeUTF("PING");
                        ok = "OK".equalsIgnoreCase(inEngine.readUTF());
                    } catch (Exception e) {
                        ok = false;
                    }
                    
                    send(outC, obj("type","HB","ts",System.currentTimeMillis(),"cp",cpId,"ok",ok));

                    // (Opcional) si CENTRAL te envía algo por el mismo socket y quieres consumirlo:
                     if (inC.available() > 0) { var ev = recv(inC); System.out.println("[MON] <- "+ev); }

                    Thread.sleep(100); // evita busy loop; HB ya va cada ~1s
                }

            } catch (Exception e) {
                System.out.println("[MON] Desconectado de CENTRAL: " + e.getMessage() + " (reintento 1s)");
                try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
            }
        }
    }
}

