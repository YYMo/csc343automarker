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
      e.printStackTrace();
    }

  }

 /* public static void main(String[] args) throws SQLException {
    Assignment2 a = new Assignment2();
    a.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3avena", "g3avena", "");
    System.out.println(a.insertCountry(13, "all", 7777, 88888888));
    System.out.println(a.getCountriesNextToOceanCount(11111111)); // -1
    System.out.println(a.getOceanInfo(0000)); // 0 sea1 222
    System.out.println(a.getOceanInfo(1111)); // 1111 sea16 888888
    System.out.println(a.getOceanInfo(11111)); // ""
    System.out.println(a.chgHDI(10, 2014, (float) 70.5));
    System.out.println(a.deleteNeighbour(9, 10));
    System.out.println(a.listCountryLanguages(10)); // 11:j:9000000#1:j:11000000#10:j:20000000#1000:j:40000000#10000:j:10000000#100:j:10000000#
    System.out.println(a.updateHeight(10, 100));
    System.out.println(a.updateDB());
  }*/



  // Using the input parameters, establish a connection to be used for this session. Returns true if
  // connection is sucessful
  public boolean connectDB(String URL, String username, String password) {

    try {
      connection = DriverManager.getConnection(URL, username, password);

    } catch (SQLException se) {
      se.printStackTrace();
    }

    return true;
  }

  // Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() {
    try {
      connection.close();

    } catch (SQLException e) {
      e.printStackTrace();
    }

    return true;
  }

  public boolean insertCountry(int cid, String name, int height, int population) {

    try {
      sql = connection.createStatement();
      String query =
          "insert into a2.country " + "values (" + cid + ",'" + name + "'," + height + ","
              + population + ")";
      sql.executeUpdate(query);
    } catch (SQLException e) {

      return false;
    }


    return true;

  }

  public int getCountriesNextToOceanCount(int oid) {
    int numcid = -1;

    try {
      ps =
          connection.prepareStatement("select oid, count(cid) " + "from A2.oceanAccess where oid="
              + oid + " group by oid");
      rs = ps.executeQuery();
      if (rs.next()) {
        numcid = rs.getInt("count");
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return numcid;

  }

  public String getOceanInfo(int oid) {
    String output = "";
    try {
      ps = connection.prepareStatement("select * from a2.ocean where oid =" + oid);
      rs = ps.executeQuery();
      if (rs.next()) {
        int oidVal = rs.getInt("oid");
        String onameVal = rs.getString("oname");
        String depthVal = rs.getString("depth");
        output = oidVal + ":" + onameVal + ":" + depthVal;
      }
    } catch (SQLException e1) {
      e1.printStackTrace();
    }
    return output;
  }

  public boolean chgHDI(int cid, int year, float newHDI) {
    try {
      sql = connection.createStatement();
      String query =
          "update a2.hdi set hdi_score=" + newHDI + " where cid=" + cid + " and year=" + year;
      sql.executeUpdate(query);
    } catch (SQLException e1) {
      return false;
    }
    return true;
  }

  public boolean deleteNeighbour(int c1id, int c2id) {
    try {
      sql = connection.createStatement();
      String query1 = "delete from a2.neighbour where country=" + c1id + " and neighbor=" + c2id;
      String query2 = "delete from a2.neighbour where country=" + c2id + " and neighbor=" + c1id;
      sql.executeUpdate(query1);
      sql.executeUpdate(query2);
    } catch (SQLException e1) {
      return false;
    }
    return true;
  }

  public String listCountryLanguages(int cid) {
    String output = "";
    try {
      sql = connection.createStatement();
      String query =
          "select l.lid,c.cname,c.population*l.lpercentage as population"
              + " from a2.language l , a2.country c  where l.cid =" + cid + " and c.cid=" + cid
              + " order by c.population";
      rs = sql.executeQuery(query);
      while (rs.next()) {
        int lidVal = rs.getInt("lid");
        String cnameVal = rs.getString("cname");
        int popVal = rs.getInt("population");
        output = output + lidVal + ":" + cnameVal + ":" + popVal + "#";
      }
    } catch (SQLException e1) {
      return "";
    }
    return output.substring(0, output.length() - 1);

  }

  public boolean updateHeight(int cid, int decrH) {
    try {
      sql = connection.createStatement();
      String query = "update a2.country set height=" + decrH + "where cid=" + cid;
      sql.executeUpdate(query);
    } catch (SQLException e1) {
      return false;
    }
    return true;

  }

  public boolean updateDB() {
    try {
      String insert = "";
      String sort = "";
      sql = connection.createStatement();
      String table =
          "CREATE TABLE a2.mostPopulousCountries"
              + "(cid INTEGER REFERENCES a2.country(cid) ON DELETE RESTRICT,"
              + "cname VARCHAR(20) NOT NULL);";
      String query = "select cid,cname from a2.country where population>100000000";
      rs = sql.executeQuery(query);
      while (rs.next()) {
        int cidVal = rs.getInt("cid");
        String cnameVal = rs.getString("cname");
        insert =
            insert + "insert into a2.mostPopulousCountries values (" + cidVal + ",'" + cnameVal
                + "');";
      }
      sort = table + insert + "select * from a2.mostPopulousCountries order by cid asc";
      sql.executeUpdate(sort);
    } catch (SQLException e1) {
      e1.printStackTrace();
      return false;
    }
    return true;
  }
}
