/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bd;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author MarcosPortatil
 */
public class Conexion {

    Connection connection;

    /**
     * Establecemos la conexion a la ip del cole junto con el usuario y
     * contraseña de ORACLE y si no puede conectar a esa, se intentará conectar
     * a la BD desde fuera del colegio con el usuario y contraseña de ORACLE
     * también.
     *
     */
    public Conexion() {
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            try {
                connection = DriverManager.getConnection("jdbc:oracle:thin:@192.168.180.10:1521:INSLAFERRERI", "MFUENTES", "1234");
            } catch (SQLException ex) {
                try {
                    connection = DriverManager.getConnection("jdbc:oracle:thin:@ieslaferreria.xtec.cat:8081:INSLAFERRERI", "MFUENTES", "1234");
                } catch (SQLException e) {
                    System.out.println(e);
                }
            }
        } catch (ClassNotFoundException cnf) {
            Logger.getLogger(Conexion.class.getName()).log(Level.SEVERE, null, cnf);
        }
    }

    /**
     * Devuelve la conexión
     *
     * @return
     */
    public Connection getConnection() {
        return connection;
    }

    /**
     * Finalizar conexión
     *
     * @throws SQLException
     */
    public void finalizarConexion() throws SQLException {
        connection.close();
    }

    /**
     * Metodo que le llega un objeto Ubicaciones y sirve para realizar un Insert
     * Into de la ubicacion Una vez hecha la sentencia sql pasamos todos los
     * datos del objeto ubicacion al PreparedStatement Convertimos la fecha para
     * que no de problemas con la BBDD al ser diferente tipo de Date I
     * devolvemos un booleano;
     *
     * @param ubicacion
     * @return
     * @throws SQLException
     * @throws ParseException
     */
    public boolean insertarUbicacion(Ubicaciones ubicacion) throws SQLException, ParseException {
        int res;
        String sql = "INSERT INTO UBICACIONES VALUES (?, ?, ?, ?)";
        PreparedStatement stmt = connection.prepareStatement(sql);

        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        stmt.setString(1, ubicacion.getMatricula());
        stmt.setDouble(2, ubicacion.getLatitud());
        stmt.setDouble(3, ubicacion.getLongitud());

        stmt.setDate(4, fechaSql(ubicacion.getData()));
        res = stmt.executeUpdate();
        return (res == 1);

    }

    /**
     * Metodo que devuelve un objeto tipo java.sql.Date y obtiene un objeto por
     * parametro de tipo fecha. Una vez tiene fecha lo transforma con un parse a
     * un objeto de tipo date y lo convierte a un sql
     *
     * @param fecha
     * @return
     * @throws ParseException
     */
    private java.sql.Date fechaSql(String fecha) throws ParseException {
        Date date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(fecha);

        return new java.sql.Date(date.getTime());
    }

    /**
     * Método para obtener todas las matriculas junto a sus contraseñas de la
     * tabla de los autobuses.
     *
     * @return
     * @throws SQLException
     */
    public List<Autobuses> obtenerAutobuses() throws SQLException {
        ResultSet rset;
        List<Autobuses> lista = new ArrayList();
        String sql = "SELECT * FROM AUTOBUSES";
        PreparedStatement stmt = getConnection().prepareStatement(sql);
        rset = stmt.executeQuery();
        while (rset.next()) {
            lista.add(new Autobuses(rset.getString("MATRICULA"), rset.getString("PASSWORD")));
        }
        finalizarConexion();
        return lista;
    }

    public Autobuses obtenerAutobus(String matricula) throws SQLException {
        Autobuses aut = null;
        ResultSet rset;
        String sql = "SELECT * FROM AUTOBUSES WHERE MATRICULA LIKE ?";

        PreparedStatement stmt = getConnection().prepareStatement(sql);
        stmt.setString(1, matricula);
        rset = stmt.executeQuery();
        while (rset.next()) {
            aut = new Autobuses(rset.getString("MATRICULA"), rset.getString("PASSWORD"));
        }
        finalizarConexion();
        return aut;
    }

    /**
     * Método que realiza la consulta para obtener la ubicación del bus con la
     * matricula que se pasa por parametro y devuelve la ubicacion de ese bus
     *
     * @param matricula
     * @return
     * @throws SQLException
     */
    public List<Ubicaciones> obtenerUbicacionBus(String matricula) throws SQLException {
        List<Ubicaciones> ubi = new ArrayList<>();
        ResultSet rset;
        String sql = "SELECT * FROM (SELECT * FROM UBICACIONES WHERE matricula LIKE ? ORDER BY FECHA DESC) WHERE ROWNUM <=5";

        PreparedStatement stmt = getConnection().prepareStatement(sql);
        stmt.setString(1, matricula);
        rset = stmt.executeQuery();
        while (rset.next()) {
            ubi.add(new Ubicaciones(rset.getString("MATRICULA"), rset.getDouble("LATITUD"), rset.getDouble("LONGITUD"), rset.getString("FECHA")));
        }
        finalizarConexion();
        return ubi;
    }

    /**
     * Mètodo que realiza la consulta para obtener las últimas posiciones de
     * todos los buses.
     *
     * @return
     * @throws SQLException
     */
    public List<Ubicaciones> obtenerUltimaPosBuses() throws SQLException {
        ResultSet rset;
        List<Ubicaciones> lista = new ArrayList<>();
        String sql = "SELECT * FROM UBICACIONES WHERE (MATRICULA, FECHA) IN (SELECT MATRICULA, MAX(FECHA) FROM UBICACIONES GROUP BY MATRICULA)";
        PreparedStatement stmt = getConnection().prepareStatement(sql);
        rset = stmt.executeQuery();
        while (rset.next()) {
            lista.add(new Ubicaciones(rset.getString("MATRICULA"), rset.getDouble("LATITUD"), rset.getDouble("LONGITUD"), rset.getString("FECHA")));
        }
        finalizarConexion();
        return lista;
    }

}
