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
    Assignment2() {}

    //Using the input parameters, establish a connection to be used for this 
    //session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {
        try {
            connection = DriverManager.getConnection(URL, username, "");
        } catch (SQLException se) {
            return false;
        }
        return true;
    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
        try {
            connection.close();
        } catch (SQLException se) {
            return false;
        }
        return true;
    }

    public boolean insertCountry(int cid, String name, int height,
        int population) {
        String queryString = "INSERT INTO a2.country " +
            "VALUES(?, ?, ?, ?)   ";
        try {
            ps = connection.prepareStatement(queryString);
        } catch (SQLException se) {
            return false;
        }

        try {
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            ps.executeUpdate();
            ps.close();
        } catch (SQLException se) {
            return false;
        }

        return true;
    }

    public int getCountriesNextToOceanCount(int oid) {
        int i = 0;
        String queryString = "SELECT COUNT(a2.oceanAccess.cid)" +
            " FROM a2.oceanAccess" +
            " where a2.oceanAccess.oid = " + oid;
        try {
            sql = connection.createStatement();
            rs = sql.executeQuery(queryString);
            if (rs.next()) {
                i = rs.getInt(1);
            }
            sql.close();
            rs.close();
        } catch (SQLException se) {
            return -1;
        }
        return i;
    }

    public String getOceanInfo(int oid) {
        String a = "", b = "", c = "", d;
        String queryString = "SELECT *      " +
            " FROM a2.ocean" +
            " WHERE a2.ocean.oid = " + oid;
        try {
            sql = connection.createStatement();
            rs = sql.executeQuery(queryString);
            if (rs.next()) {
                a = Integer.toString(rs.getInt(1));
                b = rs.getString(2);
                c = Integer.toString(rs.getInt(3));
            }
            sql.close();
            rs.close();
        } catch (SQLException e) {
            return "";
        }
        d = a + ":" + b + ":" + c;
        return d;
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
        int id, yea;
        float hdi;
        try {
            sql = connection.createStatement();
            String queryString = "UPDATE a2.hdi SET hdi_score = " + newHDI +
                " WHERE cid = " + cid + " and year = " + year;
            sql.executeUpdate(queryString);
            queryString = "SELECT * FROM a2.hdi";
            rs = sql.executeQuery(queryString);
            while (rs.next()) {
                id = rs.getInt(1);
                yea = rs.getInt(2);
                hdi = rs.getFloat(3);
            }
            sql.close();
            rs.close();
        } catch (SQLException se) {
            se.printStackTrace();
            return false;
        }
        return true;
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
        int cid, nid, length;
        try {
            sql = connection.createStatement();
            String queryString = "DELETE FROM a2.neighbour WHERE country = " + c1id +
                " and neighbor = " + c2id;
            sql.executeUpdate(queryString);
            queryString = "SELECT * FROM a2.neighbour";
            rs = sql.executeQuery(queryString);
            while (rs.next()) {
                cid = rs.getInt(1);
                nid = rs.getInt(2);
                length = rs.getInt(3);
            }
            queryString = "DELETE FROM a2.neighbour WHERE country = " + c2id +
                " and neighbor = " + c1id;
            sql.executeUpdate(queryString);
            queryString = "SELECT * FROM a2.neighbour";
            rs = sql.executeQuery(queryString);
            while (rs.next()) {
                cid = rs.getInt(1);
                nid = rs.getInt(2);
                length = rs.getInt(3);
            }
            sql.close();
            rs.close();
        } catch (SQLException se) {
            se.printStackTrace();
            return false;
        }
        return true;
    }

    public String listCountryLanguages(int cid) {
        int id, population;
        String result = "", name = "";
        try {
            sql = connection.createStatement();
            String sqlText = "SELECT lid, lname, lpercentage * population AS lpopulation " +
                "FROM a2.country INNER JOIN a2.language " +
                "ON a2.country.cid = a2.language.cid " +
                "WHERE a2.country.cid = " + cid +
                "ORDER BY lpopulation";
            rs = sql.executeQuery(sqlText);
            if (rs != null) {
                while (rs.next()) {
                    id = rs.getInt(1);
                    name = rs.getString(2);
                    population = rs.getInt(3);
                    result = result + id + ":" + name + ":" + population + "#";
                }
            }
            rs.close();
            sql.close();
        } catch (SQLException e) {
            e.printStackTrace();
            return "";
        }
        return result.substring(0, result.length() - 1);
    }

    public boolean updateHeight(int cid, int decrH) {
        int id, height, population;
        String cname = "";
        try {
            sql = connection.createStatement();
            String queryString = "UPDATE a2.country SET height = height - " + decrH +
                " WHERE cid = " + cid;
            sql.executeUpdate(queryString);
            queryString = "SELECT * FROM a2.country";
            rs = sql.executeQuery(queryString);
            while (rs.next()) {
                id = rs.getInt(1);
                cname = rs.getString(2);
                height = rs.getInt(3);
                population = rs.getInt(4);
            }
            sql.close();
            rs.close();
        } catch (SQLException se) {
            se.printStackTrace();
            return false;
        }
        return true;
    }


    public boolean updateDB() {
        int id;
        String sqlText = "";
        String name = "";
        try {
            sql = connection.createStatement();
            //sqlText = "DROP TABLE IF EXISTS mostPopulousCountries CASCADE";
            //sql.executeUpdate(sqlText);
            sqlText = "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries " +
                      "(cid INTEGER, " + 
                      " cname VARCHAR(20))";
            sql.executeUpdate(sqlText);
            sqlText = "INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 100000000" +
                      " ORDER BY cid ASC)";
            sql.executeUpdate(sqlText);
            //rs = sql.executeQuery(sqlText);
            sqlText = "SELECT * FROM a2.mostPopulousCountries";
            rs = sql.executeQuery(sqlText);
            while (rs.next()) {
                id = rs.getInt(1);
                name = rs.getString(2);
            }
            sql.close();
            rs.close();
            return true;
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        
    }


}