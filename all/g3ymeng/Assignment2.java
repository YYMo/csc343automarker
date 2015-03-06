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

  boolean debug = false;
  
  //CONSTRUCTOR
  Assignment2(){
    try {
      Class.forName("org.postgresql.Driver");
    } catch (ClassNotFoundException se) {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      System.exit(1);
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try
      {
        String dburl = "jdbc:postgresql://" + URL;
        connection = DriverManager.getConnection(dburl, username, password);

        ps = connection.prepareStatement("Set search_path to a2;");
        ps.execute();
        return true;
      }
      catch (SQLException se)
      {
        if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
        return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try
    {
      if (rs != null) { rs.close(); }
      if (connection != null) { connection.close(); }
      return true;    
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return false;
    }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    String str_cid;
    String str_height;
    String str_population;
    String query_s;
    try
    { // Update if exists
      ps = connection.prepareStatement("SELECT cid FROM country WHERE cid = ?::integer"); 
      str_cid = Integer.toString(cid);
      ps.setString(1, str_cid);
      rs = ps.executeQuery();
      if (rs.next()) {
        return false;
      }

      str_height = Integer.toString(height);
      str_population = Integer.toString(population);

      // VALUES (cid, name, height, population
      query_s = "INSERT INTO country(cid, cname, height, population) "
              + "VALUES (?::integer, ?, ?::integer, ?::integer)";
      ps = connection.prepareStatement(query_s);
      ps.setString(1, str_cid);
      ps.setString(2, name);
      ps.setString(3, str_height);
      ps.setString(4, str_population);
      ps.executeUpdate();

      ps.close();
      rs.close();
      return true;
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    String str_oid;
    String query_s;
    Integer oceanNum;
    try {
      str_oid = Integer.toString(oid);
      query_s = "SELECT DISTINCT count(cid) oceanNum FROM oceanAccess where "
              + "(oid = ?::integer) GROUP BY oid";
      ps = connection.prepareStatement(query_s);
      ps.setString(1, str_oid);
      rs = ps.executeQuery();
      while(rs.next()) {
        oceanNum = rs.getInt("oceanNum");

        ps.close();
        rs.close();
        return oceanNum;
      }
      return -1;
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return -1;
    }
  }
   
  public String getOceanInfo(int oid) {
    String query_str;
    String str_oid;
    String name;
    int depth;
    String info;

    try
    {
      str_oid = Integer.toString(oid);
      query_str = "SELECT * FROM ocean where (oid = ?::integer)";
      ps = connection.prepareStatement(query_str);
      ps.setString(1, str_oid);
      rs = ps.executeQuery();
      while (rs.next()) {
        name = rs.getString("oname");
        depth = Integer.parseInt(rs.getString("depth"));
        info = str_oid + ":" + name + ":" + depth;

        ps.close();
        rs.close();
        return info;
      }
      return "";
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return "";
    } 
  }

  public boolean chgHDI(int cid, int year, float newHDI) {
    String query_s;
    String str_cid;
    String str_year;
    String str_newHDI;
    try {
      str_cid = Integer.toString(cid);
      str_year = Integer.toString(year);
      str_newHDI = Float.toString(newHDI);

      query_s = "UPDATE hdi SET hdi_score = ?::float where "
              + "(cid = ?::integer) AND (year = ?::integer)";

      ps = connection.prepareStatement(query_s);
      ps.setString(1, str_newHDI);
      ps.setString(2, str_cid);
      ps.setString(3, str_year);
      ps.executeUpdate();

      ps.close();
      rs.close();
      return true;
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id) {
    String query_s;
    String str_c1id;
    String str_c2id;
    try
    {
      str_c1id = Integer.toString(c1id);
      str_c2id = Integer.toString(c2id);
      query_s = "DELETE FROM neighbour where (country = ?::integer) AND "
              + "(neighbor = ?::integer)";
      ps = connection.prepareStatement(query_s);
      ps.setString(1, str_c1id);
      ps.setString(2, str_c2id);
      ps.executeUpdate();

      ps = connection.prepareStatement(query_s);
      ps.setString(1, str_c2id);
      ps.setString(2, str_c1id);
      ps.executeUpdate();


      ps.close();
      rs.close();
      return true;
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return false;        
    }

  }
  
  public String listCountryLanguages(int cid) {
    String query_s;
    String str_cid;
    String res;
    String lid;
    String lname;
    Double population;
    Integer int_pop;
    try
    {
      res = new String();
      str_cid = Integer.toString(cid);
      query_s = "SELECT c.cid as cid, l.lid as lid, l.lname as lname, "
              + "l.lpercentage * c.population as population "
              + "FROM language as l, country as c WHERE "
              + "(l.cid = c.cid) AND "
              + "(l.cid = ?::integer) ORDER BY population";

      ps = connection.prepareStatement(query_s);
      ps.setString(1, str_cid);
      rs = ps.executeQuery();
      while (rs.next()) {
         lid = Integer.toString(rs.getInt("lid"));
         lname = rs.getString("lname");
         population = rs.getDouble("population");
         int_pop = population.intValue();
         res+= lid + ":" + lname + ":" + Integer.toString(int_pop);
         if (!rs.isLast()) { res += "#";}
      }

      ps.close();
      rs.close();

      return res;
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH) {
    String query_str;
    String str_cid;
    Integer height;
    try {
      str_cid = Integer.toString(cid);
      query_str = "SELECT height FROM country WHERE (cid = ?::integer)";
      ps = connection.prepareStatement(query_str);
      ps.setString(1, str_cid);

      rs = ps.executeQuery();
      // How to check if ResultSet is empty!   
      // REFERENCE: http://stackoverflow.com/questions/867194
      // /java-resultset-how-to-check-if-there-are-any-results      
      // ASKEDBY: kal
      // ANSWERBY: Seifer
      // ANSWERUR: http://stackoverflow.com/users/861203/seifer
      if (!rs.isBeforeFirst()) {    
         return false;
      }
      height = 0;
      while (rs.next()) {
         height = rs.getInt("height");
      }
      height -= decrH;
      query_str = "UPDATE country SET height = ?::integer where "
                + "(cid = ?::integer)";
      ps = connection.prepareStatement(query_str);
      ps.setString(1, Integer.toString(height));
      ps.setString(2, str_cid);
      ps.executeUpdate();

      ps.close();
      rs.close();

      return true;
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return false;
    }
  }


  public boolean updateDB() {
    String query_str;
    String str_cid;
    String country_name;
    /* (1) DROP the table
     * (2) Get Query
     * (3) Insert 1 by 1
     */
    try
    {
      // (1)
      query_str = "DROP TABLE IF EXISTS mostPopulousCountries";
      ps = connection.prepareStatement(query_str);
      ps.executeUpdate();

      // (2)
      query_str = "CREATE TABLE mostPopulousCountries ( " 
                + "cid INTEGER REFERENCES country(cid) ON DELETE RESTRICT, "
                + "cname VARCHAR(20) NOT NULL )";
      ps = connection.prepareStatement(query_str);
      ps.executeUpdate();

      // (3)
      query_str = "SELECT cid, cname FROM country WHERE (population > 100000000)";
      ps = connection.prepareStatement(query_str);
      rs = ps.executeQuery();
      while (rs.next()) {
         query_str = "INSERT INTO mostPopulousCountries (cid, cname) "
                   + "VALUES (?::integer, ?)";
         ps = connection.prepareStatement(query_str);
         ps.setString(1, Integer.toString(rs.getInt("cid")));
         ps.setString(2, rs.getString("cname"));
         ps.executeUpdate();
      }

      ps.close();
      rs.close();

      return true;
    }
    catch (SQLException se)
    {
      if (debug) { System.out.println(se.getMessage()); se.printStackTrace(System.out);}
      return false;    
    }
  }
  
}
