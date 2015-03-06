import java.sql.*;
public class Assignment2{
    
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
    } catch (ClassNotFoundException ce){
       ce.printStackTrace();
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) {
    try{
      connection = DriverManager.getConnection(URL, username, password);
      return true;
    }catch(SQLException se){
      se.printStackTrace();
      return false;
    }catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try{
      connection.close();
      if(connection.isClosed()){
       return true;
      }
      return false;
    }catch(SQLException se){
      se.printStackTrace();
      return false;
    }catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try{
      sql = connection.createStatement();
      String query = "INSERT INTO A2.country VALUES(" + cid + ", '" + name 
        + "', " + height + "," + population + ");";
      sql.executeUpdate(query);
      return true;
    }catch(SQLException se){
      se.printStackTrace();
      return false;
    }catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try{
      sql = connection.createStatement();
      String query = "SELECT COUNT(*) AS rowCount FROM A2.oceanAccess WHERE oid =" + "'" + oid + "';";
      rs = sql.executeQuery(query);
      rs.next();
      int count = rs.getInt("rowCount");
      rs.close();
      return count;
    }
    catch(SQLException se){
      se.printStackTrace();
      return -1; 
    }catch(Exception e){
      e.printStackTrace();
      return -1;
    }
  }
   
  public String getOceanInfo(int oid) {
    try{
    sql = connection.createStatement();
      String query = "SELECT * FROM A2.ocean WHERE oid =" + "'" + oid + "';";
      rs = sql.executeQuery(query);
      if (rs.next()){
        return rs.getString("oid") + ":" + rs.getString("oname") + ":" +  rs.getString("depth");
      }else{
        return "";
      }
    }catch(SQLException se){
      se.printStackTrace();
      return "";
    }catch(Exception e){
      e.printStackTrace();
      return "";
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try{
      sql = connection.createStatement();
      String query = "UPDATE A2.hdi SET hdi_score = " + newHDI + "WHERE cid = " + cid + " and year = " + year + ";";
      sql.executeUpdate(query);
	  return true;
    }catch(SQLException se){
      se.printStackTrace();
      return false;
    }catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try{
      sql = connection.createStatement();
      String query1 = "DELETE FROM A2.neighbour WHERE country = " + c1id + "and neighbor = " + c2id + ";";
      sql.executeUpdate(query1);
      String query2 = "DELETE FROM A2.neighbour WHERE country = " + c2id + "and neighbor = " + c1id + ";";
      sql.executeUpdate(query2);
	  return true;
	}catch(SQLException se){
      se.printStackTrace();
      return false;
    }catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }
  
  public String listCountryLanguages(int cid){
    try{
        sql = connection.createStatement(); 
      String query = "SELECT lid, lname, (population*lpercentage) AS lpopulation FROM A2.language NATURAL JOIN A2.country WHERE cid = " + cid + " ORDER BY lpopulation;";
      rs = sql.executeQuery(query);
      String s = "";
      if(!rs.next()){
        return "";
      }else{
        rs.next();
        s += (rs.getString("lid") + ":" + rs.getString("lname") + ":" + rs.getFloat("lpopulation"));
        while(rs.next()){
          s += ("#" + rs.getString("lid") + ":" + rs.getString("lname") + ":" + rs.getFloat("lpopulation"));
        }
      }
      return s;
    }catch(SQLException se){
      se.printStackTrace();
      return "";
    }catch(Exception e){
      e.printStackTrace();
      return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try{
      sql = connection.createStatement();
      String query1 = "SELECT height FROM A2.country WHERE cid = " + Integer.toString(cid) + ";";
      rs = sql.executeQuery(query1);
      rs.next();
      int he = rs.getInt("height");
      String query2 = "UPDATE A2.country SET height = " + Integer.toString(he - decrH) + "WHERE cid = " + Integer.toString(cid) + ";";
      sql.executeUpdate(query2);
      return true;
    }catch(SQLException se){
      se.printStackTrace();
	  return false;
    }catch(Exception e){
      e.printStackTrace();
	  return false;
    }
  }
    
  public boolean updateDB(){
    try{
        sql = connection.createStatement();
        String query1 = "CREATE TABLE a2.mostPopulousCountries (cid " +
          "INTEGER , cname VARCHAR(20) NOT NULL);";
        sql.executeUpdate(query1);
        String query2 = "INSERT INTO a2.mostPopulousCountries(SELECT cid, cname"  
         + " FROM A2.country WHERE population >= 10e7 ORDER BY cid);";
        sql.executeUpdate(query2);
		return true;
    }catch(SQLException se){
      se.printStackTrace();
      return false;
    }catch(Exception e){
      e.printStackTrace();
      return false;
    }
  }
}
