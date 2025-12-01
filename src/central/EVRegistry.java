package central;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.sql.*;
import java.util.Properties;
import java.util.concurrent.Executors;

public class EVRegistry {

    private static Properties config = new Properties();
    private static final JsonParser jsonParser = new JsonParser();

    public static void main(String[] args) throws Exception {
        String rutaConfig = "config/evregistry.config";
        try (InputStream in = Files.newInputStream(Path.of(rutaConfig))) {
            config.load(in);
        }

        int port = Integer.parseInt(config.getProperty("registry.port", "8081"));
        HttpServer http = HttpServer.create(new InetSocketAddress(port), 0);

        http.createContext("/api/registry/register", new RegisterHandler());

        http.setExecutor(Executors.newCachedThreadPool());
        http.start();

        System.out.println("[EVR] EVRegistry escuchando en puerto " + port);
    }

    // --- Handler /api/registry/register --------------------------------------

    static class RegisterHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange ex) throws IOException {
            try {
                if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
                    sendJson(ex, 405, error("Método no permitido, usa POST"));
                    return;
                }

                String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);

                JsonObject json;
                try {
                    json = jsonParser.parse(body).getAsJsonObject();
                } catch (Exception e) {
                    sendJson(ex, 400, error("JSON inválido"));
                    return;
                }

                String cpId = json.has("cpId") ? json.get("cpId").getAsString().trim() : "";
                String location = json.has("location") ? json.get("location").getAsString().trim() : "";

                if (cpId.isEmpty() || location.isEmpty()) {
                    sendJson(ex, 400, error("cpId y location son obligatorios"));
                    return;
                }

                String secret;
                try {
                    secret = findOrCreateCpSecret(cpId, location);
                } catch (SQLException sqle) {
                    sqle.printStackTrace();
                    sendJson(ex, 500, error("Error de BD: " + sqle.getMessage()));
                    return;
                }

                JsonObject resp = new JsonObject();
                resp.addProperty("status", "OK");
                resp.addProperty("cpId", cpId);
                resp.addProperty("location", location);
                resp.addProperty("secret", secret);

                sendJson(ex, 200, resp.toString());

            } catch (Exception e) {
                e.printStackTrace();
                sendJson(ex, 500, error("Error interno: " + e.getMessage()));
            }
        }
    }

    // --- Lógica de BD --------------------------------------------------------

    private static Connection getConnection() throws SQLException {
        String url  = config.getProperty("db.url");
        String user = config.getProperty("db.user");
        String pass = config.getProperty("db.pass");
        return DriverManager.getConnection(url, user, pass);
    }

    /**
     * Devuelve el secret del CP. Si no existe, lo crea.
     */
    private static String findOrCreateCpSecret(String cpId, String location) throws SQLException {
        String secret = null;

        try (Connection cn = getConnection()) {

            // 1) ¿Existe ya el CP?
            try (PreparedStatement ps = cn.prepareStatement(
                    "SELECT secret FROM EV_CP_REGISTRY WHERE cp_id = ?")) {
                ps.setString(1, cpId);
                try (ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) {
                        secret = rs.getString("secret");
                    }
                }
            }

            if (secret != null) {
                // 2) Ya existía: actualizar location y status, pero dejar mismo secret
                try (PreparedStatement ps = cn.prepareStatement(
                        "UPDATE EV_CP_REGISTRY " +
                        "   SET location = ?, status = ?, updated_at = CURRENT_TIMESTAMP " +
                        " WHERE cp_id = ?")) {
                    ps.setString(1, location);
                    ps.setString(2, "ACTIVO");
                    ps.setString(3, cpId);
                    ps.executeUpdate();
                }
                return secret;
            }

            // 3) No existía: generar secret nuevo e insertar
            secret = generateSecret();

            try (PreparedStatement ps = cn.prepareStatement(
                    "INSERT INTO EV_CP_REGISTRY " +
                    " (cp_id, location, secret, status, created_at, updated_at) " +
                    " VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)")) {
                ps.setString(1, cpId);
                ps.setString(2, location);
                ps.setString(3, secret);
                ps.setString(4, "ACTIVO");
                ps.executeUpdate();
            }

            return secret;
        }
    }

    private static String generateSecret() {
        byte[] buf = new byte[24]; // ~32 chars Base64 URL-safe
        new SecureRandom().nextBytes(buf);
        return java.util.Base64.getUrlEncoder().withoutPadding().encodeToString(buf);
    }

    // --- Utilidades HTTP / JSON ---------------------------------------------

    private static void sendJson(HttpExchange ex, int statusCode, String body) throws IOException {
        byte[] raw = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(statusCode, raw.length);
        try (OutputStream os = ex.getResponseBody()) {
            os.write(raw);
        }
    }

    private static String error(String msg) {
        JsonObject o = new JsonObject();
        o.addProperty("status", "ERROR");
        o.addProperty("error", msg);
        return o.toString();
    }
}
