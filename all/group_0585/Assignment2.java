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
    }catch(ClassNotFoundException e){
        return;
    }
  }



  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String url, String username, String password){
    try{
	  connection = DriverManager.getConnection(url, username, password);
	  ps = connection.prepareStatement("SET search_path TO A2");
	  ps.executeUpdate();
	  return true;
    }catch (SQLException e){

      return false;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
    	  connection.close();
    	  return true;
      }catch (SQLException e){
    	  return false;
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try{
		sql = connection.createStatement();
		String query = "SELECT * " +
                    "FROM country " +
                    "WHERE cid = " + cid;
      		  rs = sql.executeQuery(query);
		 if(rs.next()){

		     return false;
		  }
		else{
			ps = connection.prepareStatement("INSERT INTO country(cid,cname,height,population) VALUES(?, ?, ?, ?)");
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  ps.executeUpdate();
		  return true;
		}
	  }catch(SQLException e){
		  
		  return false;
	  }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  try{
		  String query = "SELECT count(*) AS numContries " +
		  						"FROM oceanAccess " +
		  						"GROUP BY oid " +
								"HAVING oid = " + oid;
		  sql = connection.createStatement();
		  rs = sql.executeQuery(query);
		  if(rs.next())
		  	return rs.getInt("numContries");
		  else
			return -1;					
	  }catch(SQLException e){
		  return -1;
	  }
  }
  
  public String getOceanInfo(int oid){
	  try{
		  String query = "SELECT * " +
							"FROM ocean " +
							"WHERE oid = " + oid;
		  sql = connection.createStatement();
		  rs = sql.executeQuery(query);
		  String result;
		  if (!rs.next())
			  result = "";
		  else
			  result = "" + oid + ":" + rs.getString("oname") + ":" + rs.getString("depth");
		  return result;
	  }catch(SQLException e){
		  return "";
	  }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try{
      
      sql = connection.createStatement();
      String query = "UPDATE hdi " +
                      "SET hdi_score = " + newHDI +
                      "WHERE cid = " + cid + " AND year = " + year;
      sql.executeUpdate(query);
      return true;
    }catch(SQLException e){
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try{
      sql = connection.createStatement();
      String query = "DELETE FROM neighbour " +
                    "WHERE country = " + c1id + " AND neighbor = " + c2id;
      sql.executeUpdate(query);
      String query2 = "DELETE FROM neighbour " +
                    "WHERE country = " + c2id + " AND neighbor = " + c1id;
      sql.executeUpdate(query2);
      return true;
    }catch(SQLException e){
      return false;
    }   
  }
  
  public String listCountryLanguages(int cid){
    try{
      String query = "SELECT language.cid AS lcid, language.lid AS llid, language.lname AS llname, language.lpercentage AS llper, country.cid AS ccid, country.population AS cp " +
                     "FROM language, country " +
                     "WHERE language.cid = " + cid + " AND country.cid = " + cid;
      sql = connection.createStatement();
      rs = sql.executeQuery(query);
      String result="";
      while(rs.next()){
        result = result + "|"+rs.getInt("lcid")+"|"+rs.getString("llname")+"|"
                +(rs.getInt("llper")*rs.getInt("cp")) + "#";
      }
      return result.substring(0,result.length()-1);
    }catch(SQLException e){
      return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try{
      
      String query = "SELECT * " +
		     "FROM country " +
		     "WHERE cid = " + cid;
    
      sql = connection.createStatement();
      rs = sql.executeQuery(query);
      if(rs.next()){
	int h = rs.getInt("height") - decrH;
      	String query2 = "UPDATE country " +
                      "SET height = " + h +
                      "WHERE cid = " + cid;
      	sql.executeUpdate(query2);
      	return true;
      }
       else
	return false;
    }catch(SQLException e){
      return false;
    }
  }

    
  public boolean updateDB(){
	   try{
      sql = connection.createStatement();
      String table = "CREATE TABLE mostPopulousCountries " +
                   "(cid INTEGER, " +
                   "cname VARCHAR(20))"; 
      sql.executeUpdate(table);
      String query = "SELECT * " +
                    "FROM country " +
                    "WHERE population > 100000000";
      rs = sql.executeQuery(query);
      while(rs.next()){
        ps = connection.prepareStatement("INSERT INTO mostPopulousCountries(cid,cname) VALUES(?, ?)");
        ps.setInt(1, rs.getInt("cid"));
        ps.setString(2, rs.getString("cname"));
        ps.executeUpdate();
      }
      return true;
     }catch(SQLException e){
      return false;
     }
  }
  
}
