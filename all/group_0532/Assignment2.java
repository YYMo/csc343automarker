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

  // CONSTRUCTOR
  Assignment2() {
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException e) {
      System.err.println("Failed to find the JDBC driver");
    }
  }

  // Using the input parameters, establish a connection to be used for this
  // session. Returns true if connection is successful.
  public boolean connectDB(String URL, String username, String password) {
    try {
      connection = DriverManager.getConnection(URL, username, password);
      return true;
    } catch (SQLException se) {
      System.err.println("SQL Exception: Connection unsuccessful");
    }
    return false;
  }

  // Closes the connection. Returns true if closure was successful.
  public boolean disconnectDB() {
    if (connection != null) {
      try {
        connection.close();
        return true;
      } catch (SQLException e) {
        System.err.println(
            "SQL Exception: Connection closing unsuccessful");
      }
    }
    return false;
  }

  public boolean insertCountry(int cid, String name, int height,
      int population) {
    String queryCheck = "SELECT count(*) FROM a2.country WHERE cid = ?";
    String queryString = "INSERT INTO a2.country VALUES (?, ?, ?, ?)";
    try {
      // Check if cid already exists here
      ps = connection.prepareStatement(queryCheck);
      ps.setInt(1, cid);
      
      rs = ps.executeQuery();
      if (rs.next()) {
        if (rs.getInt(1) != 0) {
          rs.close();
          ps.close();
          return false;
        }
      } else {
        rs.close();
        ps.close();
        return false;
      }
      // Insert if cid doesn't exist
      ps = connection.prepareStatement(queryString);
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population);
      
      if (ps.executeUpdate() != 1) {
        rs.close();
        ps.close();
        return false;
      }

      rs.close();
      ps.close();
    } catch (SQLException e) {
      System.err.println(e.toString());
      return false;
    }
    return true;
  }

  public int getCountriesNextToOceanCount(int oid) {
    String queryCheck = "SELECT count(*) FROM a2.ocean WHERE oid = ?";
    String queryString = "SELECT count(cid) FROM a2.oceanAccess WHERE oid = ?";
    int oceanCount = -1;
    try {
      ps = connection.prepareStatement(queryCheck);
      ps.setInt(1, oid);
      
      rs = ps.executeQuery();
      if (rs.next()) {
        if (rs.getInt(1) == 0) {
          rs.close();
          ps.close();
          return -1;
        }
      } else {
        rs.close();
        ps.close();
        return -1;
      }
      
      ps = connection.prepareStatement(queryString);
      ps.setInt(1, oid);
      
      rs = ps.executeQuery();
      if (rs.next()) {
        oceanCount = rs.getInt(1);
      }
      
      rs.close();
      ps.close();
    } catch (SQLException e) {
      System.err.println(e.toString());
      return -1;
    }
    return oceanCount;
  }

  public String getOceanInfo(int oid) {
    String queryString = "SELECT oname, depth FROM a2.ocean WHERE oid = ?";
    String result = "";
    try {
      ps = connection.prepareStatement(queryString);
      ps.setInt(1, oid);
      
      rs = ps.executeQuery();
      if (rs.next()) {
        result = oid + ":" + rs.getString(1) + ":" + rs.getInt(2);
      }
      
      rs.close();
      ps.close();
    } catch (SQLException e) {
      System.err.println(e.toString());
      return "";
    }
    return result;
  }

  public boolean chgHDI(int cid, int year, float newHDI) {
    String queryString = "UPDATE a2.hdi "
        + "SET hdi_score = ? "
        + "WHERE cid = ? "
        + "AND year = ?";
    try {
      ps = connection.prepareStatement(queryString);
      ps.setFloat(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);

      if (ps.executeUpdate() != 1) {
        ps.close();
        return false;
      }

      ps.close();
    } catch (SQLException e) {
      System.err.println(e.toString());
      return false;
    }
    return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id) {
    String queryString = "DELETE FROM a2.neighbour "
        + "WHERE country = ? "
        + "AND neighbor = ?";
    try {
      ps = connection.prepareStatement(queryString);
      ps.setInt(2, c1id);
      ps.setInt(1, c2id);

      if (ps.executeUpdate() != 1) {
        ps.close();
        return false;
      }

      ps.close();
    } catch (SQLException e) {
      System.err.println(e.toString());
      return false;
    }
    return true;
  }

  public String listCountryLanguages(int cid) {
    StringBuilder result = new StringBuilder();
    String queryString = 
        "SELECT a2.language.lid, "
            + "a2.language.lname, "
            + "a2.country.population * language.lpercentage AS lpopulation "
            + "FROM a2.language "
            + "INNER JOIN a2.country "
            + "ON a2.country.cid = a2.language.cid "
            + "WHERE a2.language.cid = ?";
    try {
      ps = connection.prepareStatement(queryString);
      ps.setInt(1, cid);
      rs = ps.executeQuery();

      if (rs != null) {
        if (rs.next()) {
          result.append(
              + rs.getInt(1)
              + ":" + rs.getString(2)
              + ":" + rs.getInt(3));
          }
        // First row doesn't start with #
        while (rs.next()) {
          result.append(
              "#" + rs.getInt(1)
              + ":" + rs.getString(2)
              + ":" + rs.getInt(3));
        }
      }
      
      ps.close();
      rs.close();
    } catch (SQLException e) {
      System.err.println(e.toString());
      return "";
    }
    return result.toString();
  }

  public boolean updateHeight(int cid, int decrH) {
    if (decrH <= 0) {
      return false;
    }
    String queryString = "UPDATE a2.country SET height=height-? WHERE cid=?";
    try {
      ps = connection.prepareStatement(queryString);
      ps.setInt(1, decrH);
      ps.setInt(2, cid);

      if (ps.executeUpdate() != 1) {
        ps.close();
        return false;
      }

      ps.close();
      return true;
    } catch (SQLException e) {
      System.err.println(e.toString());
      return false;
    }
  }

  public boolean updateDB() {
    String createTable =
        "CREATE TABLE mostPopulousCountries ("
            + "cid INTEGER, "
            + "cname VARCHAR(20))";
    String fillTable = 
        "INSERT INTO mostPopulousCountries ("
            + "SELECT cid, cname "
            + "FROM a2.country "
            + "WHERE population > 100000000)";
    try {
      sql = connection.createStatement();
      sql.executeUpdate(createTable);
      
      if (sql.executeUpdate(fillTable) == 0) {
        sql.close();
        return false;
      }
      sql.close();
    } catch (SQLException e) {
      System.err.println(e.toString());
      return false;
    }
    return true;
  }

}
