package ev_w;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URLEncoder;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * EV_W (Weather Control Office)
 *
 * - Mantiene un mapa CP -> ciudad (por consola).
 * - Cada pollMs ms:
 *      - Pide temperatura a OpenWeather.
 *      - Si temp < 0ºC: alerta=true -> POST /api/weather en CENTRAL.
 *      - Si temp vuelve a >=0ºC: alerta=false -> POST /api/weather.
 */
public class EV_W {

    private static final JsonParser JSON = new JsonParser();

    // Config
    private final String centralBaseUrl;
    private final String owApiKey;
    private final long   pollMs;

    // Estado por CP
    static class CpWeather {
        final String cpId;
        volatile String loc;          // cadena de consulta a OpenWeather, p.ej. "Alicante,ES"
        volatile Double lastTempC;    // última temperatura conocida
        volatile Boolean lastAlert;   // última alerta enviada (true/false) o null si nunca
        volatile long   lastUpdate;   // millis

        CpWeather(String cpId, String loc) {
            this.cpId = cpId;
            this.loc  = loc;
        }
    }

    private final Map<String,CpWeather> cps = new ConcurrentHashMap<>();

    private volatile boolean running = true;

    // ======================= MAIN =======================

    public static void main(String[] args) throws Exception {
        String rutaCfg = "config/evw.config";
        Properties cfg = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(rutaCfg))) {
            cfg.load(in);
        }

        String centralBaseUrl = cfg.getProperty("evw.central.baseUrl", "http://127.0.0.1:8080");
        String apiKey         = cfg.getProperty("evw.openweather.apiKey", "").trim();
        long pollMs           = parseLongOr(cfg.getProperty("evw.pollMs","4000"), 4000L);

        if (apiKey.isEmpty()) {
            System.err.println("[EV_W] FALTA evw.openweather.apiKey en " + rutaCfg);
            return;
        }

        EV_W app = new EV_W(centralBaseUrl, apiKey, pollMs);
        app.start();
    }

    private EV_W(String centralBaseUrl, String owApiKey, long pollMs) {
        this.centralBaseUrl = centralBaseUrl.replaceAll("/+$", ""); // sin barra final
        this.owApiKey       = owApiKey;
        this.pollMs         = pollMs;

        System.out.println("[EV_W] centralBaseUrl=" + this.centralBaseUrl +
                           " pollMs=" + this.pollMs + "ms");
    }

    // ======================= ARRANQUE =======================

    private void start() {
        // Hilo de polling
        Thread poller = new Thread(this::pollLoop, "evw-poller");
        poller.setDaemon(true);
        poller.start();

        // Hilo de consola (comandos ADD/DEL/LIST/QUIT)
        consoleLoop();
        running = false;
        System.out.println("[EV_W] Finalizado.");
    }

    // ======================= POLL LOOP =======================

    private void pollLoop() {
        while (running) {
            try {
                if (!cps.isEmpty()) {
                    for (CpWeather cw : cps.values()) {
                        try {
                            processOneCp(cw);
                        } catch (Exception e) {
                            System.err.println("[EV_W] Error procesando " + cw.cpId + ": " + e.getMessage());
                        }
                    }
                }
                Thread.sleep(pollMs);
            } catch (InterruptedException ie) {
                return;
            } catch (Exception e) {
                System.err.println("[EV_W] pollLoop ERROR: " + e.getMessage());
            }
        }
    }

    private void processOneCp(CpWeather cw) {
        if (cw.loc == null || cw.loc.isBlank()) return;

        Double temp = fetchTempC(cw.loc);
        if (temp == null) return; // error al consultar OpenWeather

        boolean alert = temp < 0.0;
        Boolean prevAlert = cw.lastAlert;

        cw.lastTempC   = temp;
        cw.lastUpdate  = System.currentTimeMillis();

        // Si el estado de alerta no cambia, no hace falta llamar a CENTRAL otra vez
        if (prevAlert != null && prevAlert.booleanValue() == alert) {
            return;
        }

        cw.lastAlert = alert;

        // Enviar notificación a CENTRAL
        boolean ok = sendWeatherToCentral(cw.cpId, cw.loc, temp, alert);
        System.out.printf(Locale.ROOT,
                "[EV_W] %s temp=%.2f°C alert=%s -> CENTRAL: %s%n",
                cw.cpId, temp, alert, ok ? "OK" : "FAIL");
    }

    // ======================= OPENWEATHER =======================

    /**
     * Devuelve la temperatura en ºC para la localización dada, o null si hay error.
     * loc es la cadena tipo "Alicante,ES".
     */
    private Double fetchTempC(String loc) {
        HttpURLConnection conn = null;
        try {
            String q = URLEncoder.encode(loc, StandardCharsets.UTF_8);
            String urlStr = "https://api.openweathermap.org/data/2.5/weather?q=" +
                            q + "&appid=" + owApiKey + "&units=metric";

            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("GET");
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300)
                    ? conn.getInputStream()
                    : conn.getErrorStream();

            String body = readAll(is);
            if (code < 200 || code >= 300) {
                System.err.println("[EV_W][OW] HTTP " + code + " body=" + body);
                return null;
            }

            JsonObject root = JSON.parse(body).getAsJsonObject();
            if (!root.has("main")) {
                System.err.println("[EV_W][OW] Respuesta sin 'main': " + body);
                return null;
            }
            JsonObject main = root.getAsJsonObject("main");
            if (!main.has("temp")) {
                System.err.println("[EV_W][OW] 'main' sin 'temp': " + body);
                return null;
            }
            return main.get("temp").getAsDouble();
        } catch (Exception e) {
            System.err.println("[EV_W][OW] Error consultando '" + loc + "': " + e.getMessage());
            return null;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    // ======================= CENTRAL /api/weather =======================

    private boolean sendWeatherToCentral(String cpId, String loc, double tempC, boolean alert) {
        HttpURLConnection conn = null;
        try {
            String urlStr = centralBaseUrl + "/api/weather";
            URL url = new URL(urlStr);
            conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(3000);
            conn.setReadTimeout(3000);
            conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");

            JsonObject payload = new JsonObject();
            payload.addProperty("cp", cpId);
            payload.addProperty("loc", loc);
            payload.addProperty("tempC", tempC);
            payload.addProperty("alert", alert);

            byte[] data = payload.toString().getBytes(StandardCharsets.UTF_8);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(data);
            }

            int code = conn.getResponseCode();
            InputStream is = (code >= 200 && code < 300)
                    ? conn.getInputStream()
                    : conn.getErrorStream();

            String respBody = readAll(is);
            if (code < 200 || code >= 300) {
                System.err.println("[EV_W][CENTRAL] HTTP " + code + " body=" + respBody);
                return false;
            }
            return true;
        } catch (Exception e) {
            System.err.println("[EV_W][CENTRAL] Error POST /api/weather: " + e.getMessage());
            return false;
        } finally {
            if (conn != null) conn.disconnect();
        }
    }

    // ======================= CONSOLA =======================

    private void consoleLoop() {
        System.out.println("""
            [EV_W] Comandos:
              ADD <cpId> <localizacion>
                  Ej: ADD CP-001 "Alicante,ES"
              DEL <cpId>
              LIST
              HELP
              QUIT
            """);
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {
            String line;
            while (running && (line = br.readLine()) != null) {
                line = line.trim();
                if (line.isEmpty()) continue;

                String upper = line.toUpperCase(Locale.ROOT);
                if (upper.equals("QUIT") || upper.equals("EXIT")) {
                    break;
                }
                if (upper.equals("HELP")) {
                    System.out.println("ADD <cpId> <loc> | DEL <cpId> | LIST | QUIT");
                    continue;
                }
                if (upper.equals("LIST")) {
                    listCps();
                    continue;
                }
                if (upper.startsWith("ADD ")) {
                    handleAdd(line.substring(4).trim());
                    continue;
                }
                if (upper.startsWith("DEL ")) {
                    handleDel(line.substring(4).trim());
                    continue;
                }

                System.out.println("[EV_W] Comando desconocido. Usa HELP.");
            }
        } catch (Exception e) {
            System.err.println("[EV_W] consoleLoop ERROR: " + e.getMessage());
        }
    }

    private void handleAdd(String rest) {
        // rest = "<cpId> <loc...>"
        if (rest.isEmpty()) {
            System.out.println("[EV_W] Uso: ADD <cpId> <loc>");
            return;
        }
        String[] parts = rest.split("\\s+", 2);
        if (parts.length < 2) {
            System.out.println("[EV_W] Uso: ADD <cpId> <loc>");
            return;
        }
        String cpId = parts[0].toUpperCase(Locale.ROOT);
        String loc  = parts[1].trim();
        CpWeather existing = cps.get(cpId);
        if (existing == null) {
            cps.put(cpId, new CpWeather(cpId, loc));
            System.out.println("[EV_W] Añadido CP=" + cpId + " loc='" + loc + "'");
        } else {
            existing.loc = loc;
            System.out.println("[EV_W] Actualizado CP=" + cpId + " loc='" + loc + "'");
        }
    }

    private void handleDel(String cpIdRaw) {
        if (cpIdRaw == null || cpIdRaw.isBlank()) {
            System.out.println("[EV_W] Uso: DEL <cpId>");
            return;
        }
        String cpId = cpIdRaw.toUpperCase(Locale.ROOT);
        CpWeather removed = cps.remove(cpId);
        if (removed != null) {
            System.out.println("[EV_W] Eliminado CP=" + cpId);
        } else {
            System.out.println("[EV_W] CP no encontrado: " + cpId);
        }
    }

    private void listCps() {
        if (cps.isEmpty()) {
            System.out.println("[EV_W] Sin CPs configurados. Usa ADD.");
            return;
        }
        System.out.println("[EV_W] CPs configurados:");
        for (CpWeather cw : cps.values()) {
            String tempStr = (cw.lastTempC == null) ? "-" :
                    String.format(Locale.ROOT,"%.2f°C", cw.lastTempC);
            String alertStr = (cw.lastAlert == null) ? "-" : cw.lastAlert.toString();
            System.out.printf("  %s -> %s  temp=%s alert=%s%n",
                    cw.cpId, cw.loc, tempStr, alertStr);
        }
    }

    // ======================= HELPERS =======================

    private static long parseLongOr(String s, long def) {
        try { return Long.parseLong(s); } catch (Exception e) { return def; }
    }

    private static String readAll(InputStream is) throws Exception {
        if (is == null) return "";
        try (BufferedReader br = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
            StringBuilder sb = new StringBuilder();
            String line;
            while ((line = br.readLine()) != null) sb.append(line);
            return sb.toString();
        }
    }
}
