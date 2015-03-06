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
    Assignment2() {
        try {//Load JDBC driver
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e) {
            ;
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password) {
        try {
            connection = DriverManager.getConnection(URL, username, password);
            this.sql = this.connection.createStatement();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB() {
        try {
            sql.close();
            connection.close();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public boolean insertCountry(int cid, String name, int height, int population) {
        String sqlText = "INSERT INTO Country values(?, ?, ?, ?);";
        String cidExists = "SELECT * FROM Country WHERE Country.cid = " + cid + ";";
        try {
            rs = sql.executeQuery(cidExists);
            if (rs.next() == false) {
                ps = connection.prepareStatement(sqlText);
                ps.setInt(1, cid);
                ps.setString(2, name);
                ps.setInt(3, height);
                ps.setInt(4, population);
                ps.executeUpdate();
                ps.close();
                rs.close();
            } else {
                rs.close();
                return false;
            }
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public int getCountriesNextToOceanCount(int oid) {
        int numOfCountries = -1;
        String sqlText = "SELECT count(*) AS countCountries FROM oceanAccess WHERE oid = " + oid + ";";

        try {
            rs = sql.executeQuery(sqlText);
            if (rs.next()) {
                numOfCountries = rs.getInt("countCountries");
                rs.close();
            }
        } catch (SQLException e) {
            return numOfCountries;
        }
        return numOfCountries;
    }

    public String getOceanInfo(int oid) {
        String oceanInfo = "";
        String sqlText = "SELECT oid, oname, depth FROM ocean WHERE oid = " + oid + ";";

        try {
            rs = sql.executeQuery(sqlText);
            if (rs.next()) {
                String oname = rs.getString("oname");
                int depth = rs.getInt("depth");
                oceanInfo = oid + ":" + oname + ":" + depth;
                rs.close();
            }
        } catch (SQLException e) {
            return oceanInfo;
        }
        return oceanInfo;
    }

    public boolean chgHDI(int cid, int year, float newHDI) {
        String sqlText = "UPDATE hdi SET hdi_score = " + newHDI + " WHERE cid = " + cid + " AND year = " + year + ";";
        try {
            sql.executeUpdate(sqlText);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public boolean deleteNeighbour(int c1id, int c2id) {
        String sqlText1 = "DELETE FROM neighbour WHERE country = " + c1id + " AND neighbour = " + c2id + ";";
        String sqlText2 = "DELETE FROM neighbour WHERE country = " + c2id + " AND neighbour = " + c1id + ";";
        try {
            sql.executeUpdate(sqlText1);
            sql.executeUpdate(sqlText2);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public String listCountryLanguages(int cid) {
        String returnQuery = "";
        String viewCountryLanguage = "CREATE VIEW CountryLanguage AS " +
                                     "SELECT population, country.cid AS cid, lid, lname, lpercentage " +
                                     "FROM country, language " +
                                     "WHERE country.cid = language.cid;";
        String sqlText = "SELECT lid, lname, (lpercentage*population) AS langPopulation " +
                         "FROM CountryLanguage " +
                         "WHERE cid = " + cid +
                         " SORT BY langPopulation DESC;";
        try {
            sql.executeUpdate(viewCountryLanguage);
            rs = sql.executeQuery(sqlText);
            while (rs.next()) {
                int lid = rs.getInt("lid");
                String lname = rs.getString("lname");
                int population = rs.getInt("langPopulation");
                returnQuery += lid + ":" + lname + ":" + population + "#";
            }
            if (returnQuery != "") {
                returnQuery = returnQuery.substring(0, returnQuery.length() - 1);
            }
            sql.executeUpdate("DROP VIEW CountryLanguage");
            rs.close();
        } catch (SQLException e) {
            return returnQuery;
        }
        return returnQuery;
    }

    public boolean updateHeight(int cid, int decrH) {
        String sqlText = "UPDATE country " +
                         "SET height = height - " + decrH +
                         " WHERE cid = " + cid + ";";
        try {
            sql.executeUpdate(sqlText);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public boolean updateDB() {
        String sqlText1 = "SELECT cid, cname " +
                          "FROM country " +
                          "WHERE population > 100000000 " +
                          "ORDER BY cid ASC;";
        String sqlText2 = "CREATE TABLE a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20));";
        try {
            sql.executeUpdate(sqlText2);
            String sqlText3 = "INSERT INTO a2.mostPopulousCountries(" + sqlText1 + ");";
            sql.executeUpdate(sqlText3);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }
}