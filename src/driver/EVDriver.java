package driver;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class EVDriver {

    private static int parseIntOr(String s, int def) {
        try {
            return Integer.parseInt(s);
        }
        catch (Exception e) {
            return def;
        }
    }

    public static void main(String[] args) throws Exception {

        String rutaConfig = "config/driver.config";
        Properties config = new Properties();
        Path ruta = Path.of(rutaConfig).toAbsolutePath().normalize();

        try (InputStream fichero = Files.newInputStream(ruta)) {
            config.load(fichero);
        } catch (NoSuchFileException e) {
            System.err.println("[DRV] No encuentro el fichero de configuración: " + ruta);
            System.exit(1);
            return;
        } catch (Exception e) {
            System.err.println("[DRV] Error leyendo configuración en " + ruta + ": " + e.getMessage());
            System.exit(1);
            return;
        }

        String host = config.getProperty("driver.centralHost", "127.0.0.1");
        int port = parseIntOr(config.getProperty("driver.centralPort"), 5000);
        String driverID = config.getProperty("driver.id", "D-001");
        String filePath = config.getProperty("driver.file", "").trim();
        int socketTimeout = parseIntOr(config.getProperty("driver.socketTimeout"), 0);

        System.out.println("[DRV] Config: " + ruta);
        System.out.println("[DRV] Conectado a CENTRAL " + host + ": " + port + " como " + driverID);

        List<String> cpList = null;
        if (!filePath.isEmpty()) {
            Path f = Path.of(filePath).toAbsolutePath().normalize();
            if (!Files.isRegularFile(f)) {
                System.err.println("[DRV] AVISO: driver.file apunta a un fichero que no existe: " + f);
            }
            else {
                System.out.println("[DRV] Modo fichero: " + f);
                cpList = new ArrayList<>();
                try (var lines = Files.lines(f, StandardCharsets.UTF_8)) {
                    lines.map(String::trim).filter(l -> !l.isEmpty() && !l.startsWith("#")).forEach(cpList::add);
                }
                if (cpList.isEmpty()) {
                    System.out.println("[DRV] El fichero está vacío. Paso a modo manual.");
                }
            }
        }
        try (Socket s = new Socket(host, port);
             var in = new DataInputStream(s.getInputStream());
             var out = new DataOutputStream(s.getOutputStream());
             var br = new BufferedReader(new InputStreamReader(System.in))) {

            if (socketTimeout > 0) s.setSoTimeout(socketTimeout);
                
            if (cpList != null) {
                //--Modo fichero: procesa cada linea con 4s entre peticiones
                for (int i = 0; i < cpList.size(); i++){
                    String cp = cpList.get(i);
                    String req = "REQ_START " + driverID + " " + cp;
                    System.out.println("[DRV] (" + (i+1) + "/" + cpList.size() + ") -> " + req);
                    out.writeUTF(req);

                    String ans = in.readUTF(); // AUTH_OK ... | AUTH_NO ...
                    mostrarRespuesta(ans, cp);
                    if (ans.startsWith("AUTH_OK")) {
                        // Esperar TICKET de fin de sesión
                        while (true) {
                            String ev = in.readUTF();
                            if (ev.startsWith("TICKET")) {
                                System.out.println("[DRV] " + ev);
                                break;
                            } 
                            else {
                                System.out.println("[DRV] (evento) " + ev);
                            }
                        }
                        System.out.println("[DRV] Esperando 4s antes del siguiente servicio…");
                        Thread.sleep(4000);
                    }
                }
                System.out.println("[DRV] Fichero procesado. Fin.");
            }
            else {
                //--Modo manual
                System.out.println("Conectado. Introduce CP-ID (Presiona Enter vacío para salir): ");
                while (true) {
                    System.out.println("CP-ID> ");
                    String cp = br.readLine();
                    if (cp == null || cp.isBlank()) {
                        System.out.println("[DRV] Saliendo.");
                        break;
                    }

                    String req = "REQ_START " + driverID + " " + cp.trim();
                    System.out.println("[DRV] -> " + req);
                    out.writeUTF(req);

                    String ans = in.readUTF();
                    mostrarRespuesta(ans, cp);

                    if (ans.startsWith("AUTH_OK")) {
                        while (true) {
                            String ticket = in.readUTF();          // TICKET u otros eventos
                            if (ticket.startsWith("TICKET")) {
                                System.out.println("[DRV] " + ticket);
                                break;                            // volvemos a pedir CP-ID
                            } 
                            else {
                                System.out.println("[DRV] (evento) " + ticket);
                            }
                        }
                    }
                }
            }
        }
        catch (Exception e) {
            System.out.println("[DRV] Conexión perdida o error de I/O: " + e.getMessage());           
        }
    }

    private static void mostrarRespuesta (String ans, String cp){
        if (ans == null){
            System.out.println("[DRV] ? Respuesta nula");
            return;
        }
        else if (ans.startsWith("AUTH_OK")) {
            String[] p = ans.trim().split("\\s+");
            String sesion="(?)", cpAns=cp, precio="(?)";
            if (p.length == 3) {              // antiguo: AUTH_OK <sesion> <precio>
                sesion = p[1]; precio = p[2];
            } 
            else if (p.length >= 4) {       // nuevo:   AUTH_OK <sesion> <cp> <precio>
                sesion = p[1]; cpAns = p[2]; precio = p[3];
            }
            System.out.println("[DRV] AUTORIZADO: sesión " + sesion + " en " + cpAns + " (precio " + precio + ")");
        }

        else if (ans.startsWith("AUTH_NO")) {
            String motivo = ans.substring("AUTH_NO".length()).trim();
            System.out.println("[DRV] DENEGADO: " + (motivo.isEmpty() ? "DESCONOCIDO" : motivo));
        }
        else {
            System.out.println("[DRV] ? Respuesta inesperada:  " + ans);
        }
    }
}
