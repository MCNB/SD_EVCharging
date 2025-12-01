package cp_monitor;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static common.net.Wire.*;

public class CPMonitor {

    private static final JsonParser JSON = new JsonParser();

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
        final int    centralPort = getInt(p, "monitor.centralPort", 5000);
        final String cpId        = p.getProperty("monitor.cpId", "CP-001");
        final String ubic        = p.getProperty("monitor.ubicacion", "N/A");
        final double precio      = getDouble(p, "monitor.precio", 0.35);

        final String engineHost  = p.getProperty("monitor.engineHost", "127.0.0.1");
        final int    enginePort  = getInt(p, "monitor.enginePort", 6100);

        final String registryUrl =
                p.getProperty("registry.url", "http://127.0.0.1:8081/api/registry/register");
        final String secretFile =
                p.getProperty("registry.secret.file", "config/registry.secret");
        final String keyFile =
                p.getProperty("auth.key.file", "config/cp.key");

        System.out.printf("[MON] cfg central=%s:%d cp=%s ubic=%s precio=%.2f engine=%s:%d%n",
                centralHost, centralPort, cpId, ubic, precio, engineHost, enginePort);

        // --- PASO 1: Registrar CP en EVRegistry (idempotente) ---
        registrarEnRegistry(cpId, ubic, registryUrl, secretFile);

        // Leemos el secret que EVRegistry nos ha dado
        final String cpSecret = leerSecret(secretFile);
        if (cpSecret == null || cpSecret.isEmpty()) {
            System.err.println("[MON] NO se ha podido leer el secret del CP. Revisa Registry.");
            return;
        }

        // --- Bucle de siempre hablando con CENTRAL (ahora con AUTH_CP al principio) ---
        for (;;) {
            try (Socket sC = new Socket(centralHost, centralPort);
                 DataInputStream  inC  = new DataInputStream(sC.getInputStream());
                 DataOutputStream outC = new DataOutputStream(sC.getOutputStream())) {

                System.out.println("[MON] Conectado a CENTRAL " + centralHost + ":" + centralPort);

                // Timeout más largo para la fase de autenticación
                sC.setSoTimeout(2000);

                // PASO 2: Autenticación en CENTRAL (AUTH_CP) y obtención de la clave simétrica
                String cpKey = autenticarEnCentral(cpId, cpSecret, inC, outC, keyFile);

                System.out.println("[MON] Autenticación OK. Clave simétrica=" + cpKey);

                // Después de la autenticación ya podemos volver a un timeout corto para drainAcks
                sC.setSoTimeout(200);

                // REG_CP (JSON vía Wire) - como en Release 1
                send(outC, obj("type","REG_CP","ts",System.currentTimeMillis(),
                               "cp",cpId,"loc",ubic,"price",precio));
                // Drena ACK (si Central lo envía)
                drainAcks(inC, /*maxFrames*/ 2, /*label*/ "REG_CP");

                long lastHb = 0L;

                while (true) {
                    long now = System.currentTimeMillis();
                    if (now - lastHb >= 1000) { // <-- 1 HB por segundo
                        boolean ok;
                        try {
                            InetSocketAddress addr = new InetSocketAddress(engineHost, enginePort);
                            try (Socket sEngine = new Socket()) {
                                sEngine.connect(addr, /*connect-timeout ms*/ 200);
                                sEngine.setSoTimeout(/*read-timeout ms*/ 200);

                                try (DataInputStream  inEngine  = new DataInputStream(sEngine.getInputStream());
                                     DataOutputStream outEngine = new DataOutputStream(sEngine.getOutputStream())) {
                                    outEngine.writeUTF("PING");
                                    ok = "OK".equalsIgnoreCase(inEngine.readUTF());
                                }
                            }
                        } catch (Exception e) {
                            ok = false; // si falla o expira: reporta KO pero NO bloquea el HB
                        }

                        // Enviar siempre el HB (aunque el health haya sido KO o haya expirado)
                        send(outC, obj("type","HB","ts",now,"cp",cpId,"ok",ok));
                        lastHb = now;

                        // Drenar ACK(s) si los hubiera (opcional)
                        drainAcks(inC, /*maxFrames*/ 2, "HB");
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

    // ------------------- AUTH_CP hacia CENTRAL -------------------

    /**
     * Envía AUTH_CP con {cp,secret} y espera AUTH_OK/AUTH_ERR.
     * Si es OK, guarda la clave simétrica en keyFile y la devuelve.
     */
    private static String autenticarEnCentral(String cpId,
                                              String cpSecret,
                                              DataInputStream inC,
                                              DataOutputStream outC,
                                              String keyFile) throws Exception {
        System.out.println("[MON] Autenticando CP en CENTRAL. cp=" + cpId);

        // Enviar AUTH_CP
        send(outC, obj("type","AUTH_CP",
                       "ts",System.currentTimeMillis(),
                       "cp",cpId,
                       "secret",cpSecret));

        // Esperar respuesta
        Object frame = recv(inC);   // Wire.recv devuelve normalmente un JsonObject (Gson)
        JsonObject resp = (JsonObject) frame;

        String type = resp.get("type").getAsString();
        if ("AUTH_OK".equals(type)) {
            String key = resp.get("key").getAsString();
            System.out.println("[MON] AUTH_OK recibido. key=" + key);

            // Guardar la clave simétrica para que la use el Engine
            Path f = Path.of(keyFile);
            if (f.getParent() != null) {
                Files.createDirectories(f.getParent());
            }
            Files.writeString(f, key.trim(), StandardCharsets.UTF_8);
            System.out.println("[MON] Clave simétrica guardada en " + f.toAbsolutePath());

            return key;
        } else if ("AUTH_ERR".equals(type)) {
            String reason = resp.has("reason") ? resp.get("reason").getAsString() : "desconocido";
            throw new RuntimeException("AUTH_ERR desde CENTRAL: " + reason);
        } else {
            throw new RuntimeException("Respuesta inesperada en AUTH_CP: type=" + type);
        }
    }

    // ------------------- Registro en EVRegistry (API REST) -------------------

    private static void registrarEnRegistry(String cpId,
                                            String ubic,
                                            String registryUrl,
                                            String secretFile) {
        try {
            System.out.println("[MON] Registrando CP en EVRegistry: " + registryUrl +
                               " cpId=" + cpId + " loc=" + ubic);

            // Construimos JSON con Wire.obj
            Object reqJson = obj("cpId", cpId, "location", ubic);
            byte[] body = reqJson.toString().getBytes(StandardCharsets.UTF_8);

            URL url = new URL(registryUrl);
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

            try (DataOutputStream out = new DataOutputStream(conn.getOutputStream())) {
                out.write(body);
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300)
                    ? conn.getInputStream()
                    : conn.getErrorStream();

            StringBuilder sb = new StringBuilder();
            try (BufferedReader br = new BufferedReader(
                    new InputStreamReader(is, StandardCharsets.UTF_8))) {
                String line;
                while ((line = br.readLine()) != null) {
                    sb.append(line);
                }
            }
            String respBody = sb.toString();

            JsonObject resp = JSON.parse(respBody).getAsJsonObject();
            String status = resp.has("status") ? resp.get("status").getAsString() : "";

            if (!"OK".equalsIgnoreCase(status)) {
                String err = resp.has("error") ? resp.get("error").getAsString() : "desconocido";
                System.err.println("[MON] ERROR Registry: " + err +
                                   " (HTTP " + code + ")");
                return;
            }

            String secret = resp.get("secret").getAsString();
            System.out.println("[MON] Registry OK. cpId=" + cpId +
                               " loc=" + ubic +
                               " secret=" + secret);

            // Guardar secret en fichero
            Path f = Path.of(secretFile);
            if (f.getParent() != null) {
                Files.createDirectories(f.getParent());
            }
            Files.writeString(f, secret.trim(), StandardCharsets.UTF_8);

            System.out.println("[MON] secret guardado en " + f.toAbsolutePath());

        } catch (Exception e) {
            System.err.println("[MON] Error registrando CP en EVRegistry: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String leerSecret(String secretFile) {
        try {
            Path f = Path.of(secretFile);
            if (!Files.exists(f)) {
                System.err.println("[MON] Fichero de secret no existe: " + f.toAbsolutePath());
                return null;
            }
            String s = Files.readString(f, StandardCharsets.UTF_8).trim();
            System.out.println("[MON] Secret leído de " + f.toAbsolutePath());
            return s;
        } catch (Exception e) {
            System.err.println("[MON] Error leyendo secret: " + e.getMessage());
            return null;
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
                System.out.println("[MON] drainAcks(" + label + ") " + e.getMessage());
                break;
            }
        }
    }
}
