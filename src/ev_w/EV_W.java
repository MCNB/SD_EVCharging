package ev_w;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import common.bus.EventBus;
import common.bus.KafkaBus;

import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import static common.net.Wire.obj;

public class EV_W {

    private static int parseIntOr(String s, int def){ try{ return Integer.parseInt(s);}catch(Exception e){return def;} }

    public static void main(String[] args) throws Exception {
        String rutaConfig = "config/evw.config";
        Properties cfg = new Properties();
        try (InputStream in = Files.newInputStream(Path.of(rutaConfig))) { cfg.load(in); }

        String centralUrl = cfg.getProperty("evw.centralUrl","http://127.0.0.1:8080");
        String apiKey     = cfg.getProperty("openweather.apiKey","");
        int intervalSec   = parseIntOr(cfg.getProperty("openweather.intervalSec","300"),300);

        final String T_TELEMETRY = cfg.getProperty("kafka.topic.telemetry","ev.telemetry.v1");

        if (apiKey == null || apiKey.isBlank()) {
            System.err.println("[EV_W] openweather.apiKey vacío. Desactivado.");
            return;
        }

        EventBus bus = KafkaBus.from(cfg);
        System.out.println("[EV_W] centralUrl=" + centralUrl + " intervalo=" + intervalSec +
                           "s topicTEL=" + T_TELEMETRY);

        Thread loop = new Thread(() -> {
            JsonParser parser = new JsonParser();
            while (true) {
                try {
                    // 1) Pedimos lista de CPs a CENTRAL
                    String cpsJson = httpGet(centralUrl + "/api/cps");
                    JsonObject root = parser.parse(cpsJson).getAsJsonObject();
                    JsonArray items = root.getAsJsonArray("items");

                    for (int i = 0; i < items.size(); i++) {
                        JsonObject cp = items.get(i).getAsJsonObject();
                        String cpId = cp.get("cp").getAsString();
                        String loc  = cp.has("loc") ? cp.get("loc").getAsString() : "";
                        String estado = cp.has("estado") ? cp.get("estado").getAsString() : "";

                        if (loc == null || loc.isBlank()) continue;
                        if ("DESCONECTADO".equalsIgnoreCase(estado)) continue;

                        // 2) Consultamos OpenWeather por ciudad
                        WeatherResult w = fetchWeather(apiKey, loc);
                        if (w == null) continue;

                        boolean alert = w.tempC < 0.0; // criterio simple: T < 0°C => alerta

                        System.out.printf("[EV_W] cp=%s loc=%s temp=%.2f alert=%s%n",
                                cpId, loc, w.tempC, alert);

                        // 3) Publicamos evento WEATHER por Kafka
                        JsonObject msg = obj(
                                "type","WEATHER",
                                "src","EV_W",
                                "ts",System.currentTimeMillis(),
                                "cp",cpId,
                                "tempC",w.tempC,
                                "alert",alert
                        );
                        bus.publish(T_TELEMETRY, cpId, msg);

                        // pequeña pausa entre CPs para no abusar del API
                        Thread.sleep(1000);
                    }

                } catch (Exception e) {
                    System.err.println("[EV_W] Loop error: " + e.getMessage());
                }

                try { Thread.sleep(intervalSec * 1000L); }
                catch (InterruptedException ie) { return; }
            }
        }, "evw-loop");

        loop.setDaemon(true);
        loop.start();

        // mantener viva la JVM
        Thread.currentThread().join();
    }

    private static String httpGet(String urlStr) throws Exception {
        HttpURLConnection conn = (HttpURLConnection) new URL(urlStr).openConnection();
        conn.setConnectTimeout(3000);
        conn.setReadTimeout(5000);
        conn.setRequestMethod("GET");
        int code = conn.getResponseCode();
        try (InputStream in = (code >= 200 && code < 300)
                ? conn.getInputStream()
                : conn.getErrorStream()) {
            if (in == null) throw new RuntimeException("HTTP " + code);
            byte[] buf = in.readAllBytes();
            return new String(buf, StandardCharsets.UTF_8);
        }
    }

    private static class WeatherResult {
        final double tempC;
        WeatherResult(double t){ this.tempC=t; }
    }

    private static WeatherResult fetchWeather(String apiKey, String city) {
        try {
            String q = java.net.URLEncoder.encode(city, StandardCharsets.UTF_8);
            String url = "https://api.openweathermap.org/data/2.5/weather?q="
                    + q + "&units=metric&appid=" + apiKey;
            String json = httpGet(url);
            JsonObject root = JsonParser.parseString(json).getAsJsonObject();
            double temp = root.getAsJsonObject("main").get("temp").getAsDouble();
            return new WeatherResult(temp);
        } catch (Exception e) {
            System.err.println("[EV_W] fetchWeather(" + city + "): " + e.getMessage());
            return null;
        }
    }
}
