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
      } catch (ClassNotFoundException ex) {
          
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session.
  //Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password){
      try {
          connection = DriverManager.getConnection(URL, username, password);
    	            
      } catch (SQLException ex) {   
          return false;
      }
      
      return true;
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
      try {
          connection.close();
      } catch (SQLException ex) {
          return false;
      }
      return true;    
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      try {

          sql = connection.createStatement();
          rs = sql.executeQuery("SELECT * FROM a2.country WHERE cid=" + cid + ";");
          
          if (rs.next()) {
              rs.close();
              sql.close();
              return false;
          } else {
              String sqlText;
              sqlText = "INSERT INTO a2.country VALUES(" + cid + ", '" + name 
                      + "', " + height + ", " + population + ");";
              sql.executeUpdate(sqlText);
              sql.close();
              rs.close();
              return true;
          }              
      } catch (SQLException ex) {
          return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      String sqlText;
      sqlText = "SELECT OA.oid, COUNT(OA.cid) AS numofcountries FROM "
              + "a2.oceanAccess AS OA GROUP BY OA.oid;";
      try {
    	  sql = connection.createStatement();
          rs = sql.executeQuery(sqlText);
          if (rs != null) {
              while (rs.next()) {
                  if (rs.getInt("oid") == oid) {
                      int result = rs.getInt("numofcountries");
                      rs.close();
                      sql.close();
                      return result;
                  }
              }
          }
          rs.close();
          sql.close();
      } catch (SQLException ex) {
          return -1;
      }
      
	return -1;    
  }
   
  public String getOceanInfo(int oid){
	  String sqlText;
      sqlText = "SELECT * FROM a2.ocean;";
      try {
    	  sql = connection.createStatement();
          rs = sql.executeQuery(sqlText);
          if (rs != null) {
              while (rs.next()) {
                  if (rs.getInt("oid") == oid) {
                      int resultOID = rs.getInt("oid");
                      String resultOName = rs.getString("oname");
                      String resultDepth = rs.getString("depth");
                      String result = Integer.toString(resultOID) + ":" + resultOName + ":" + resultDepth;
                      rs.close();
                      sql.close();
                      return result;
                  }
              }
          }
          rs.close();
          sql.close();
      } catch (SQLException ex) {
          return "";
      }
      
	return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  String sqlText;
	  sqlText = "UPDATE a2.hdi SET hdi_score=" + newHDI + 
	  			" WHERE cid=" + cid + " and year=" + year + ";";
	  
	  try {
		  sql = connection.createStatement();
		  sql.executeUpdate(sqlText);
		  
		  if (sql.getUpdateCount() >= 1) {
			  sql.close();
			  return true;
		  } else {
			  sql.close();
			  return false;
		  }

	} catch (SQLException e) {
		  return false;
	}
	  

  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  String sqlText;
	  sqlText = "SELECT * FROM a2.neighbour AS N WHERE N.country=" +
	  c1id + " and N.neighbor=" + c2id + " UNION " +
	  "SELECT * FROM a2.neighbour AS N WHERE N.country=" + c2id 
	  + " and N.neighbor=" + c1id + ";";
	  
	  try {
		  sql = connection.createStatement();
		  rs = sql.executeQuery(sqlText);
		  
		  String sqlDelete;
		  int rowsAffected = 0;
		  
		  if (rs != null) {
			  while (rs.next()) {
				  sqlDelete = "DELETE FROM a2.neighbour AS N WHERE N.country=" 
					  + rs.getInt("country") + " and N.neighbor=" + rs.getInt("neighbor") + ";";
				  
				  Statement sql2 = connection.createStatement();
				  sql2.executeUpdate(sqlDelete);
				  sql2.close();
				  rowsAffected += 1;
				  
			  }
			  
		  }	  
		  
		  sql.close(); 
		  rs.close();	
		  
		  if (rowsAffected > 0) {
			  return true;
		  } else{
			  return false;
		  }

	} catch (SQLException e) {
		  return false;
	}
       
  }
  
  public String listCountryLanguages(int cid){
	
      String sqlText;
      sqlText = "SELECT L.lid AS lid, L.lname AS lname, "
                + "(C.population*L.lpercentage) AS population FROM "
                + "a2.country AS C, a2.language as L WHERE C.cid=" + cid 
                + " and C.cid=L.cid;";
      try {
          sql = connection.createStatement();
          rs = sql.executeQuery(sqlText);
          
          String result = "";
          
          if (rs != null) {
              while (rs.next()) {
                  result += Integer.toString(rs.getInt("lid")) + ":" +
                          rs.getString("lname") + ":" +
                          Integer.toString(rs.getInt("population")) + "#";
              }
              
              if (result.length() != 0) {
                  result = result.substring(0, result.length()-1);
              }
              rs.close();
              sql.close();
              return result;
              
          }
          
      } catch (SQLException ex) {
          return "";

      }
      
      return "";
      
  }
  
  public boolean updateHeight(int cid, int decrH){
    
      String sqlText;
      sqlText = "SELECT C.cid AS cid, C.height AS height FROM a2.country AS "
              + "C WHERE C.cid=" + cid + ";";
      
      
      try {
          sql = connection.createStatement();
          rs = sql.executeQuery(sqlText);
          
          String sqlUpdate;
          int rowsAffected = 0;
          
          if (rs != null) {
              while (rs.next()) {
                  
                  sqlUpdate = "UPDATE a2.country SET height=" +
                          (rs.getInt("height") - decrH) + " WHERE cid=" + cid +
                          ";";
                  
                  Statement sql2 = connection.createStatement();
                  sql2.executeUpdate(sqlUpdate);
                  rowsAffected += 1;
                  sql2.close();
                  
              }
          }
          
          sql.close(); 
          rs.close();	
		  
	  if (rowsAffected > 0) {
              return true;
	  } else{
              return false;
	  }
          
      } catch (SQLException ex) {
          return false;
      }
          
	  
  }
    
  public boolean updateDB(){
	
      String deleteTable;
      deleteTable = "DROP TABLE IF EXISTS a2.mostPopulousCountries;";
      
      String createTable;
      createTable = "CREATE TABLE a2.mostPopulousCountries(cid int, cname "
              + "varchar(20));";
      
      String countriesToAdd;
      countriesToAdd = "SELECT C.cid AS cid, C.cname AS cname FROM a2.country"
              + " AS C WHERE C.population>100000000;";
      
      try {
          sql = connection.createStatement();
          sql.executeUpdate(deleteTable);
          sql.close();
          sql = connection.createStatement();
          sql.executeUpdate(createTable);
          sql.close();
          
          String sqlInsert;
          int rowsAffected = 0;
          
          sql = connection.createStatement();
          rs = sql.executeQuery(countriesToAdd);
          
          if (rs != null) {
              while (rs.next()) {
                  sqlInsert = "INSERT INTO a2.mostPopulousCountries VALUES(" + 
                          rs.getInt("cid") + ", '" + rs.getString("cname") +
                          "');";
                  
                  Statement sqlInsertData = connection.createStatement();
                  sqlInsertData.executeUpdate(sqlInsert);
                  rowsAffected += 1;
                  sqlInsertData.close();
              }
          }
          
          sql.close();
          rs.close();
          
          if (rowsAffected > 0) {
              return true;
          } else {
              return false;
          }
          
      } catch (SQLException ex) {
          return false;
      }
      
	  
  }
}

