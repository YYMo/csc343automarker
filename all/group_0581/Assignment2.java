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
      catch (ClassNotFoundException e) {
      System.out.println("Failed to find the JDBC driver");
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try {
        connection = DriverManager.getConnection(URL, username, password);
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }

      if (connection != null){
        return true;
      } else {
        return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
        connection.close();
        return true;
      } catch (SQLException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      return false;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   String query = "insert into country values "
       + "(" + cid + "," + name + "," + height + "," + population +");";
   try {
     sql = connection.createStatement();
     sql.executeUpdate(query);
     return true;
  } catch (SQLException e) {
    e.printStackTrace();
  }
   return false;
  }
  
  public int  getCountriesNextToOceanCount(int oid) {
    String query = "select count(cid) from oceanAccess where oid = " + oid + ";";
    try {
      sql = connection.createStatement();
      rs = sql.executeQuery(query);
      return rs.getInt(1);
      
   } catch (SQLException e) {
     e.printStackTrace();
   }
    return -1;
  }
   
  public String getOceanInfo(int oid){
    String query = "select * from ocean where oid = " + oid + ";";
    try {
      sql = connection.createStatement();
      rs = sql.executeQuery(query);
      return rs.getInt(1)+":"+rs.getString(2) + ":" + rs.getInt(3);
      
   } catch (SQLException e) {
     e.printStackTrace();
   }
    return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    String query = "Update hdi set hdi_score = ? where cid=? and year=?";
    try {
      ps = connection.prepareStatement(query);
      ps.setFloat(1, newHDI);
      ps.setInt(2, cid);
      ps.setInt(3, year);
      ps.executeUpdate();
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
    }
   return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    String query = "delete from neighbour where (country = ? and neightbour = ?) or"
        + " (neighbour = ? and country = ?)";
    try {
      ps = connection.prepareStatement(query);
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      ps.setInt(3, c1id);
      ps.setInt(3, c2id);
      ps.executeUpdate();
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
    }
   return false;      
  }
  
  public String listCountryLanguages(int cid){
    String query = "select lid, lname, (population*lpercentage) as pop "
        + "from language,country where cid = " + cid + " order by pop;";
    String result = "";
    try {
      sql = connection.createStatement();
      rs = sql.executeQuery(query);
      while(rs.next()){
        result += rs.getInt(1);
        result += ":";
        result += rs.getString(2);
        result += ":";
        result += rs.getInt(3);
        result += "#";
      }
   } catch (SQLException e) {
     e.printStackTrace();
   }
    if(result.endsWith("#")){
      result = result.substring(0, result.length()-1);
    }
    return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
    String query = "Update country set height = ? where cid=?";
    try {
      ps = connection.prepareStatement(query);
      ps.setFloat(1, decrH);
      ps.setInt(2, cid);
      ps.executeUpdate();
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
    }
   return false;
  }
    
  public boolean updateDB(){
    String query = "create table mostPopulousCountries as ("
        + "select cid,cname from country"
        + " where population > 100000000 order by cid asc";
    try {
      ps = connection.prepareStatement(query);
      return true;
    } catch (SQLException e) {
      e.printStackTrace();
    }
   return false; 
  }
  
}
