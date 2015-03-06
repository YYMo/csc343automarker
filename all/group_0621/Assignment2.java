import java.sql.*;

public class Assignment2 {

    // A connection to the database  
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;

    // Resultset for the query
    ResultSet rs;

    //CONSTRUCTOR
    Assignment2(){
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e){
            e.printStackTrace();
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
        } catch (SQLException e){
            e.printStackTrace();
        }
        return connection != null;
    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try {
            connection.close();
            return true;
        } catch (SQLException e) {

            e.printStackTrace();
        }
        return false;
    }

    public boolean insertCountry (int cid, String name, int height, int population) {
        if (name == null || name.length() > 20){
            return false;
        }
        try {
            sql = connection.createStatement();

            String sqlText;
            sqlText = "INSERT INTO a2.country " +
                      "VALUES (?, ?, ?, ?)";
            ps = connection.prepareStatement(sqlText);
            //sql.executeUpdate(sqlText);
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            ps.executeUpdate();
            ps.close();
            return true;
        } catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    public int getCountriesNextToOceanCount(int oid) {
        try {
            sql = connection.createStatement();

            String sqlText;
            sqlText = "SELECT COUNT(cid) " +
                      "FROM a2.oceanAccess " +
                      "WHERE oid=?";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            if (rs != null) {
                rs.next();
                int val = rs.getInt(1);
                ps.close();
                rs.close();
                return val;
            }
            rs.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return -1;
    }

    public String getOceanInfo(int oid){
        try {
            String sqlText;
            sqlText = "SELECT oid, oname, depth " +
                      "FROM a2.ocean " +
                      "WHERE oid=?";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            if (rs != null) {
                rs.next();
                String val = rs.getInt(1) + ":" +
                             rs.getString(2) + ":" +
                             rs.getInt(3);
                ps.close();
                rs.close();
                return val;
            } else {
                return "";
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return "";
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        try {
            String sqlText;
            sqlText = "DELETE FROM a2.hdi " +
                      "WHERE cid=? and year=?";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, cid);
            ps.setInt(2, year);
            ps.executeUpdate();
            ps.close();
            sqlText = "INSERT INTO a2.hdi " +
                      "VALUES (?, ?, ?)";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, cid);
            ps.setInt(2, year);
            ps.setFloat(3, newHDI);
            ps.executeUpdate();
            ps.close();
            return true;
        } catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            String sqlText;
            sqlText = "DELETE FROM a2.neighbour " +
                      "WHERE country=? and neighbor=?";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.executeUpdate();
            ps.setInt(1, c2id);
            ps.setInt(2, c1id);
            ps.executeUpdate();
            ps.close();
            return true;
        } catch (SQLException e){
            e.printStackTrace();
        }
        return false;
    }

    public String listCountryLanguages(int cid){
        try {
            String sqlText;
            sqlText = "SELECT language.lid, language.lname, " +
                      "(country.population * language.lpercentage) " +
                      "AS population " +
                      "FROM a2.language JOIN a2.country ON language.cid = country.cid " +
                      "WHERE language.cid=? " +
                      "ORDER BY population DESC";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, cid);
            rs = ps.executeQuery();
            if (rs != null) {
                String val = "";
                while (rs.next()) {
                    val = val.concat(rs.getString(1) + ":" +
                          rs.getString(2) + ":" +
                          rs.getString(3));
                }
                ps.close();
                rs.close();
                val = val.concat("#");
                return val.substring(0, val.length() - 1);
            } else {
                return "";
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    return "";
    }

    public boolean updateHeight(int cid, int decrH){
        try {
            String sqlText;
            sqlText = "UPDATE a2.country " +
                      "SET height = (height - ?) " +
                      "WHERE cid = ?";
            ps = connection.prepareStatement(sqlText);
            ps.setInt(1, cid);
            ps.setInt(2, decrH);
            ps.executeUpdate();
            ps.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }

    public boolean updateDB(){
        try {
            sql = connection.createStatement();
            String sqlText;
            sqlText = "CREATE TABLE a2.mostPopulousCountries ( " +
                      "cid INTEGER REFERENCES a2.country(cid) ON DELETE RESTRICT, " +
                      "cname VARCHAR(20) NOT NULL)";
            sql.executeUpdate(sqlText);
            sqlText = "INSERT INTO a2.mostPopulousCountries " +
                      "(SELECT cid, cname FROM a2.country " +
                      "WHERE population > 100000000) " +
                      "ORDER BY population ASC";
            sql.executeUpdate(sqlText);
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return false;
    }
}