package common.db;
import java.sql.*;
public class SqlServer {
  private final String url, user, pass;
  public SqlServer(String url, String user, String pass){ this.url=url; this.user=user; this.pass=pass; }
  public Connection get() throws SQLException { return DriverManager.getConnection(url, user, pass); }
}