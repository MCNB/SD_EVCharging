package cp_monitor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.net.Socket;
import java.net.SocketTimeoutException; // <— para el timeout al drenar ACKs
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

        for (;;) {
            try (Socket sC = new Socket(centralHost, centralPort);
                 DataInputStream  inC  = new DataInputStream(sC.getInputStream());
                 DataOutputStream outC = new DataOutputStream(sC.getOutputStream())) {

                // Para que un recv(...) no bloqueante falle rápido si no hay ACK
                sC.setSoTimeout(200);

                System.out.println("[MON] Conectado a CENTRAL " + centralHost + ":" + centralPort);

                // REG_CP (JSON vía Wire)
                send(outC, obj("type","REG_CP","ts",System.currentTimeMillis(),
                               "cp",cpId,"loc",ubic,"price",precio));
                // Drena ACK (si Central lo envía)
                drainAcks(inC, /*maxFrames*/ 2, /*label*/ "REG_CP");

                long lastHb = 0L;

                while (true) {
                    long now = System.currentTimeMillis();
                    if (now - lastHb >= 1000) { // <-- 1 HB por segundo
                        // Health check puntual (abre/cierra para simplificar y evitar fugas)
                        boolean ok;
                        try (Socket sEngine = new Socket(engineHost, enginePort);
                            DataInputStream  inEngine  = new DataInputStream(sEngine.getInputStream());
                            DataOutputStream outEngine = new DataOutputStream(sEngine.getOutputStream())) {
                            // JSON health ping (compatible con el Engine nuevo)
                            send(outEngine, obj("type","PING","ts",System.currentTimeMillis()));
                            var resp = recv(inEngine);
                            ok = (resp != null)
                                 && resp.has("type") && "PONG".equalsIgnoreCase(resp.get("type").getAsString())
                                 && (!resp.has("ok") || resp.get("ok").getAsBoolean());
                        } catch (Exception e) { ok = false; }

                        // Enviar HB
                        send(outC, obj("type","HB","ts",now,"cp",cpId,"ok",ok));
                        lastHb = now;

                        // Drenar ACK(s) de HB (Central suele responder 1 ACK por HB)
                        drainAcks(inC, /*maxFrames*/ 4, /*label*/ "HB");
                    }

                    // Evita busy-loop
                    Thread.sleep(50);
                }

            } catch (Exception e) {
                System.out.println("[MON] Desconectado de CENTRAL: " + e.getMessage() + " (reintento 1s)");
                try { Thread.sleep(1000); } catch (InterruptedException ignore) {}
            }
        }
    }

    /**
     * Intenta leer y descartar hasta 'max' frames con timeout corto (SO_TIMEOUT del socket).
     * Evita que se acumulen los ACK de Central en el buffer TCP.
     */
    private static void drainAcks(DataInputStream in, int max, String label) {
        for (int i = 0; i < max; i++) {
            try {
                var ack = common.net.Wire.recv(in);
                // Si quieres verlos:
                System.out.println("[MON] <- " + label + " ACK: " + ack);
            } catch (SocketTimeoutException te) {
                break; // no hay más data pendiente ahora mismo
            } catch (Exception e) {
                // Si hay error real en framing, lo registramos y salimos
                System.out.println("[MON] drainAcks(" + label + ") " + e.getMessage());
                break;
            }
        }
    }
}


