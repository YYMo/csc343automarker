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
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
          connection = DriverManager.getConnection(URL, username, password);
      } catch (SQLException e) { //Connection Failed!
          e.printStackTrace();

      }
      if (connection != null) { //Connected to database!
          return true;
      } else{ //Connection failed!
          return false;
      }
  }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
          connection.close();
          if (connection.isClosed()) {
              return true;
          }
      } catch (SQLException e) {
          e.printStackTrace();
          return false;
      }
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
      String sqlText;
      sqlText = "INSERT INTO country " + "VALUES (?, ?, ?, ?) ";
      String queryCheck = "SELECT cid FROM country";
      sql = connection.createStatement();
      rs = sql.executeQuery(queryCheck); //executes query
      if(rs.absolute(1)){
          connection.close(); //if cid exists, quit and close connection
          return false;
      } else{ //else, insert row
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          ps.close();

          int count = ps.getUpdateCount();
          if (count == 1){
              return true; //insert was successful!
          } else{
              return false;
          }
      }
  }

  public int getCountriesNextToOceanCount(int oid) {
        String queryCheck;
        queryCheck = "SELECT COUNT(cid) AS 'count' FROM oceanAccess WHERE oid = ?";
        try {
            sql = connection.createStatement();
            ps = connection.prepareStatement(queryCheck);
            ps.setInt(1, oid);
            rs = ps.executeQuery();
        } catch (SQLException e) {
            e.printStackTrace();
            return -1;
        }
        int count = rs.getInt("count");
        return count;

  }

  public String getOceanInfo(int oid){
      String sqlQuery;
      sqlQuery = "SELECT oid, oname, depth FROM ocean WHERE oid = ?";
      sql = connection.createStatement();
      ps = connection.prepareStatement(sqlQuery);
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      if (rs.next() == false){
          return ""; //return empty string if oid doesn't exist
      } else{
          String oname = rs.getString("oname");
          int depth = rs.getInt("depth");
          return oid + ":" + oname + ":" + depth;
      }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      String query = "SELECT hdi_score FROM hdi WHERE cid = ? and year = ?";
      sql = connection.createStatement();
      ps = connection.prepareStatement(query);
      ps.setInt(1, cid);
      ps.setInt(2, year);
      rs = ps.executeQuery();
      if (rs.next() == false){
          return false; //return false if cid not in hdi table 
      } else{
          try{

              String delete = "delete from hdi where cid = " + cid;
              Statement statement = connection.createStatement();
              statement.execute(delete); //executes delete sql statement
          } catch (SQLException e){
              e.printStackTrace();
          }
          String sqlText = "INSERT INTO hdi VALUES (?, ?, ?) ";
          ps = connection.prepareStatement(sqlText);
          ps.setInt(1, cid);
          ps.setInt(2, year);
          ps.setFloat(3, newHDI);
          ps.close();

          int count = ps.getUpdateCount();
          if (count == 1){
              return true; //insert was successful!
          } else{
              return false;
          }
     }

  }

  public boolean deleteNeighbour(int c1id, int c2id){
      try{
          String delete1 = "delete from neighbour where country = " + c2id + " and neighbor = " + c1id;
          String delete2 = "delete from neighbour where country = " + c1id + " and neighbor = " + c2id;
          Statement statement = connection.createStatement();
          statement.execute(delete1); //executes delete1 sql statement
          statement.execute(delete2); //executes delete2 sql statement
      } catch (SQLException e){
          e.printStackTrace();
      }
      int count = ps.getUpdateCount();
      if (count == 0){
          return false; //delete wasn't successful!
      } else{
          return true; //delete successful!
      }
  }

  public String listCountryLanguages(int cid){
      String query = "SELECT lid, lname, (lpercentage * country.population) AS 'population' FROM country, language WHERE country.cid = " + cid + " and language.cid = " + cid + "ORDER BY (lpercentage * country.population) DESC";
       ps = connection.prepareStatement(query);
       rs = ps.executeQuery();
       String finalstr = "";
       while (rs.next()) { //goes through each tuple in result
           int lid = rs.getInt("lid");
           String lname = rs.getString("lname");
           int population = rs.getInt("population");
           finalstr = finalstr + lid + ":" + lname + ":" + population + "#";
        }
        return finalstr;
  }

  public boolean updateHeight(int cid, int decrH){
      String query = "SELECT cname, height, population FROM country WHERE cid = " + cid;
      ps = connection.prepareStatement(query);
      rs = ps.executeQuery();
      String cname = rs.getString("cname");
      int height = rs.getInt("height"); //get height value
      int population = rs.getInt("population");
      try{
          String delete = "delete from country where cid = " + cid;
          Statement statement = connection.createStatement();
          statement.execute(delete); //executes delete sql statement
      } catch (SQLException e){
          e.printStackTrace();
      }
      int heightnew = height - decrH; //change height
      String sqlText = "INSERT INTO country VALUES (?, ?, ?, ?)";
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      ps.setString(2, cname);
      ps.setInt(3, heightnew);
      ps.setInt(4, population);
      ps.close();

      int count = ps.getUpdateCount();
      if (count == 1){
          return true; //height update was successful!
       } else{
           return false;
       }
  }

  public boolean updateDB(){
       String newtable = "CREATE TABLE mostPopulousCountries (cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL)";
       String query = "SELECT cid, cname FROM country WHERE population > 100000000 ORDER BY cid ASC";
      ps = connection.prepareStatement(query);
      rs = ps.executeQuery();
      Statement state = null;
      state.executeUpdate(newtable); //creates table
      String sqlText = "INSERT INTO mostPopulousCountries VALUES (?, ?)";
      ps = connection.prepareStatement(sqlText);
      while (rs.next()) {

          int cid = rs.getInt("cid");
          String cname = rs.getString("cname");
          ps.setInt(1, cid);
          ps.setString(2, cname);
      }
      ps.close();

      int count = ps.getUpdateCount();
      if (count == 0){
          return false;
       } else{
           return true; //update successful!
       }
  }

}
                                                              232,1         Bot

