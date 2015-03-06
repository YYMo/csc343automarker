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
    
    String driverName = "org.postgresql.Driver";     
    
    try {
      Class.forName(driverName);
    } catch (ClassNotFoundException e) {
	     System.out.println("Where is your PostgreSQL JDBC Driver?");
	     e.printStackTrace();
	     return;
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    
    try {
      connection = DriverManager.getConnection(URL, username, password);
      set_search_path();
    } catch (SQLException e) {
      System.out.println("Connection Failed! Check output console");
      e.printStackTrace();
    }
    
    if (connection != null) {
      return true;
    } else {
      return false;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      connection.close();
    } catch (SQLException e) {
      System.out.println("Disconnection Failed");
      e.printStackTrace();
      return false;
    }
    return true;
  }
  
  
  public void set_search_path(){
	  try{
		    sql = connection.createStatement();
		    sql.executeUpdate("SET search_path TO a2");
		  }catch (SQLException e) {
                System.out.println("set path error");
                e.printStackTrace();
			}
		}
    
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   
   String sqlText;
   
   try{
    sql = connection.createStatement();
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
   
    try{
      sqlText = "INSERT INTO country"+
		  "VALUES ("+ cid +", "+ " '" + name +
		  "' " + "," + height + "," +
		  population + ")";

       sql.executeUpdate(sqlText);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
   return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    int result = 0;
    String sqlText;
   
    try{
      sql = connection.createStatement();
      } catch (SQLException e) {
	    e.printStackTrace();
	    return -1;
      }
    
      try{
	sqlText = "SELECT count(*) AS result"+
		  "FROM oceanAccess"+
		  "WHERE oid="+oid;

	rs = sql.executeQuery(sqlText);
	
		if (rs != null) {
		  while (rs.next()) {
			result = rs.getInt("result");
		  }
		}
	
      } catch (SQLException e) {
	e.printStackTrace();
	return -1;
     }
	
	return result;  
  }
   
  public String getOceanInfo(int oid){
   String result = "";
    String sqlText;
   
    try{
      sql = connection.createStatement();
      } catch (SQLException e) {
	e.printStackTrace();
	return "";
      }
       try{
	sqlText = "SELECT * FROM ocean WHERE oid = " + oid;

	rs = sql.executeQuery(sqlText);
	
	if (rs != null) {
	  while (rs.next()) {
	    result += rs.getInt("oid") + ":" + rs.getString("oname")+ ":" + rs.getInt("depth");
	  }
	}
	
      } catch (SQLException e) {
	e.printStackTrace();
	return "";
     }
     return result;
   
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   String sqlText;
   
    try{
      sql = connection.createStatement();
      } catch (SQLException e) {
	e.printStackTrace();
	return false;
      }
    try{
      sqlText = "UPDATE hdi SET hdi_score = " + newHDI +
		  "WHERE cid = " + cid + "AND year = " + year;

       sql.executeUpdate(sqlText);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
   return true;
   
   
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    String sqlText;
   
    try{
      sql = connection.createStatement();
      } catch (SQLException e) {
	e.printStackTrace();
	return false;
      }
    try{
      sqlText = "DELETE FROM neighbour WHERE country = " + c1id + "AND neighbor = " +c2id
               + " OR " + "country = " + c2id + "AND neighbor = " +c1id;

       sql.executeUpdate(sqlText);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
   return true;       
  }
  
  public String listCountryLanguages(int cid){
    String result = "";
    String sqlText;
   
    try{
      sql = connection.createStatement();
      } catch (SQLException e) {
	e.printStackTrace();
	return "";
      }
       try{
	sqlText = "SELECT lid, lname, (lpercentage*population) AS followers" + 
	"FROM language JOIN country ON language.cid = country.cid" +
	"WHERE language.cid = " + cid;

	rs = sql.executeQuery(sqlText);
	
	if (rs != null) {
	  while (rs.next()) {
	    result += rs.getInt("lid") + ":" + rs.getString("lname")+ ":" + rs.getInt("followers") + "#";
	  }
	}
	
      } catch (SQLException e) {
	e.printStackTrace();
	return "";
     }
     return result;
   
  }
  
  public boolean updateHeight(int cid, int decrH){
    String sqlText;
   
    try{
      sql = connection.createStatement();
      } catch (SQLException e) {
	e.printStackTrace();
	return false;
      }
    try{
      sqlText = "UPDATE country SET height = height - " + decrH +
		  "WHERE cid = " + cid ;

       sql.executeUpdate(sqlText);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
   return true;       
  }
  
    
  public boolean updateDB(){
   String sqlText1;
   String sqlText11;
   
    try{
      sql = connection.createStatement();
      } catch (SQLException e) {
	e.printStackTrace();
	return false;
      }
    try{
      sqlText1 = "CREATE TABLE mostPopulousCountries (" +
      "cid INTEGER, cname VARCHAR(20))";

       sql.executeUpdate(sqlText1);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
    try{
      sqlText11 = "INSER INTO mostPopulousCountries (" +
      "SELECT cid, name FROM country WHERE population >= 100000000)";

       sql.executeUpdate(sqlText11);
    } catch (SQLException e) {
      e.printStackTrace();
      return false;
    }
   return true;           
}
}
