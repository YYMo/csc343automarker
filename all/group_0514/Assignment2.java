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
    }
    catch (ClassNotFoundException cnfe) {
      return;
    }

  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
        this.connection = DriverManager.getConnection(URL, username, password);
        return true;
      }
      catch (Exception ex) {
        return false;
      }      
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
        this.ps.close();
        this.rs.close();
        this.connection.close();
        return true;
      } 
      catch (Exception ex) {
        return false;
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {

    try {
      this.ps = connection.prepareStatement("INSERT INTO a2.country VALUES (?, ?, ?, ?);");
      // set the values in the PreparedStatement object
      this.ps.setInt(1, cid);
      this.ps.setString(2, name);
      this.ps.setInt(3, height);
      this.ps.setInt(4, population);

      // the amount of rows affected by the statement (We expect this to be one)
      int num_changed = ps.executeUpdate(); 
      if (num_changed != 1) {
        return false;
      }
      return true;
    }
    catch (SQLException ex) {
      return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try {
      this.ps = connection.prepareStatement("SELECT COUNT(*) FROM a2.oceanaccess WHERE oid = ?");
      // set the values in the PreparedStatement object
      this.ps.setInt(1, oid);

      int num_countries_next_to_ocean = -1;
      this.rs = ps.executeQuery();
      while (this.rs.next()) {
        num_countries_next_to_ocean = this.rs.getInt(1);
      }
      // value didn't update; something went wrong.
      if (num_countries_next_to_ocean < 0) {
        return -1;
      }
      return num_countries_next_to_ocean;
    }
    catch (SQLException ex) {
      return -1;  
    }
  }
   
  public String getOceanInfo(int oid){
    String result = "";
    try {
      this.ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
      // set the values in the PreparedStatement object
      this.ps.setInt(1, oid);
      this.rs = ps.executeQuery();

      while (this.rs.next()) {
        int res_oid = this.rs.getInt("oid");
        String oname = this.rs.getString("oname");
        int depth = this.rs.getInt("depth");
        result = res_oid + ":" + oname + ":" + depth;
      }
      return result;
    }
    catch (SQLException ex) {
      return "";  
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try {
      this.ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?");
      // set the values in the PreparedStatement object
      this.ps.setFloat(1, newHDI);
      this.ps.setInt(2, cid);
      this.ps.setInt(3, year);

      int num_changed = ps.executeUpdate();

      if (num_changed != 1) {
        return false;
      }
      return true;
    }
    catch (SQLException ex) {
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try {
      this.ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country=? AND neighbor=?; DELETE FROM a2.neighbour WHERE country=? AND neighbor=?");
      // set the values in the PreparedStatement object
      this.ps.setInt(1, c1id);
      this.ps.setInt(2, c2id);
      this.ps.setInt(3, c2id);
      this.ps.setInt(4, c1id);

      int num_changed = ps.executeUpdate();

      if (num_changed != 1) {
        return false;
      }
      return true;
    }
    catch (SQLException ex) {
      return false;
    }      
  }
  
  public String listCountryLanguages(int cid){
    try {
      boolean found = false;
      String result = "";
      String query = "SELECT lid, lname, (lpercentage*population) AS population ";
      query += "FROM a2.language l JOIN a2.country c ON l.cid=c.cid WHERE l.cid=? ORDER BY population;";
      this.ps = connection.prepareStatement(query);
      // set the values in the PreparedStatement object
      this.ps.setInt(1, cid);

      this.rs = ps.executeQuery();
      while (this.rs.next()) {
        found = true;
        int lid = this.rs.getInt("lid");
        String lname = this.rs.getString("lname");
        int population = this.rs.getInt("population");
        result += lid + ":" + lname + ":" + population + "#";
      }
      if (found == true) {
        result = result.substring(0, result.length() - 1);
      }
      
      return result;
    }
    catch (SQLException ex) {
      return "";
    }     
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
      int height= -1;
      boolean found = false;
      this.ps = connection.prepareStatement("SELECT height FROM a2.country WHERE cid = ?");
      this.ps.setInt(1, cid);
      this.rs = this.ps.executeQuery();
      while (this.rs.next()) {
        found = true;
        height = this.rs.getInt("height");
      }
      if (found == false) {
        return false;
      }
      this.ps = connection.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?;");
      // set the values in the PreparedStatement object
      this.ps.setInt(1, height-decrH);
      this.ps.setInt(2, cid);

      int num_changed = ps.executeUpdate();

      if (num_changed != 1) {
        return false;
      }
      return true;
    }
    catch (SQLException ex) {
      return false;
    }      
  }
    
  public boolean updateDB(){
    try {
      int res = -1;
      boolean found = false;
      this.ps = connection.prepareStatement("SELECT cid, cname INTO a2.mostPopulousCountries FROM a2.country WHERE population > 100000000 ORDER by cid ASC;");

      res = this.ps.executeUpdate();
      if (res < 0) {
        return false;
      }
      return true;
    }
    catch (SQLException ex) {
      return false;
    }        
  }
}
