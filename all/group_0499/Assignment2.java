

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
  try{
        Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException e){
        e.printStackTrace();
        }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
          
          connection = DriverManager.getConnection( URL, username , password);
          sql = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
          sql.executeUpdate("SET search_path TO A2;");
          sql.close();
      }
      catch (SQLException e){
          
          return false;
      }
      return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
          connection.close();
          return true;
      }
      catch (SQLException e){
          return false;
      }
      
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try {
          sql = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
          sql.executeUpdate("INSERT INTO country (cid, cname, height, population) VALUES ("
                  +cid+", '"+name+"', "+height+", "+population+");");
          //I might be wrong, but SQLException should be raised if cid already exists since it
          //is a unique key for this table, thus no need to "check" for existing cid.
          
          sql.close();
          return true;
      }
      catch (SQLException e) {
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          return false;
      }
   
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      int total;
      try {
          sql = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
          rs = sql.executeQuery("SELECT cid FROM oceanAccess WHERE oid="+oid+";");
          if (rs.last()) {
              total = rs.getRow();
              sql.close();
              return total;
          }
          else {
              sql.close();
              return 0;
          }
      }
      catch (SQLException e) {
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          return -1;
      }
    }
   
  public String getOceanInfo(int oid){
      String info;
      try {
          sql = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
          rs = sql.executeQuery("SELECT * FROM ocean WHERE oid="+oid+";");
          if (rs.last()) {
              info = rs.getString("oid")+":"+rs.getString("oname")+":"+rs.getString("depth");
              sql.close();
              return info;
          }
          else {
              sql.close();
              return "";
          }
      }
      catch (SQLException e){
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          return "";
      }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      try {
          sql = connection.createStatement();
          sql.executeUpdate("UPDATE hdi SET hdi_score = " + newHDI + " WHERE cid="+cid+"AND year="+year+";");
              sql.close();
              return true;

      }
      catch (SQLException e){
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          return false;
      }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      try {
          sql = connection.createStatement();
          sql.executeUpdate("DELETE FROM Neighbour WHERE country="+c1id+"AND neighbor="
                  +c2id+";");
          sql.executeUpdate("DELETE FROM Neighbour WHERE country="+c2id+"AND neighbor="
                  +c1id+";");
          sql.close();
          return true;

      }
      catch (SQLException e) {
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          return false;
      }        
  }
  
  public String listCountryLanguages(int cid){
      String result = "";
      try {
          sql = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
          rs = sql.executeQuery("SELECT DISTINCT(b.lid), a.lname, b.pop FROM language a  JOIN (SELECT a.lid, SUM(b.population*a.lpercentage) pop FROM language a JOIN country b ON a.cid=b.cid WHERE a.cid= '"+ cid +"' GROUP BY a.lid) b ON a.lid=b.lid ORDER by b.pop DESC;");
          while (rs.next()) {
              result = result+rs.getString(1)+":"+rs.getString(2)+":"+rs.getString(3)+"#";
          }
          sql.close();
          return result;
      }
      catch (SQLException e) {
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          return "";
      }
  }
  
  public boolean updateHeight(int cid, int decrH){
      try{
          sql = connection.createStatement();
          sql.executeUpdate("UPDATE country SET  height= (SELECT height FROM country WHERE cid = '"+ cid +"')-" + decrH + " WHERE cid="+cid+";");
              sql.close();
              return true;
      }
      catch (SQLException e) {
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          return false;
      }
  }
    
  public boolean updateDB(){
      try {
          Statement sql1 = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_UPDATABLE);
          rs = sql1.executeQuery(("SELECT cid, cname FROM country WHERE population>=100000000 ORDER BY cid ASC;"));
          sql = connection.createStatement();
          sql.executeUpdate("DROP TABLE IF EXISTS mostPopulousCountries CASCADE;");
          sql.executeUpdate("CREATE TABLE mostPopulousCountries ("+
                            "cid        INTEGER     PRIMARY KEY, "+
                            "cname      VARCHAR(20) NOT NULL);");
            while (rs.next()){            
          sql.executeUpdate("INSERT INTO mostPopulousCountries  VALUES (" + rs.getInt(1) +" , " +rs.getString(2) + ");");
        }
           sql.close();
          return true;
      }
      catch (SQLException e) {
          try {
              sql.close();
           }
           catch (SQLException f) {
           }
          
          return false;
      }
  }
  
}

