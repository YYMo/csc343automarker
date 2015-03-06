import java.sql.*;
import java.util.*;

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
     } catch(ClassNotFoundException e){
	System.out.println("postgresql driver not found");
     }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
	connection = DriverManager.getConnection(URL, username, password);
      };
      catch (SQLException e){
	system.out.println("Connection Failed! Check output console");
	e.printStackTrace();
      }
      if (connection != null){
	System.out.println("You're connected");
	return true;
      }
      System.out.println("Fail to make connection!");
      return false;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
	connection.close();
	return true;
      } catch (SQLException e){
	return false;  
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
      String sql = "insert into a2.country(cid,cname,height,population) values(?,?,?,?);";
      try{
	ps = connection.prepareStatement(sql);
	ps.setInt(1, cid);
	ps.setString(2, name);
	ps.setInt(3, height);
	ps.setInt(4, population);
	if(ps.executeUpdate() > 0){
	  return true;
	}
      } catch ((SQLException e){
	 return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      String sql = "SELECT COUNT * AS vals FROM a2.oceanAcess WHERE oid = ?;";
      try{
	ps = connection.prepareStatement(sql);
	ps.setInt(1, oid);
	rs = ps.executeQuery();
	rs.next();
	return rs.getInt("vals");
      } catch ((SQLException e){
	 return -1;
	}
  }
   
  public String getOceanInfo(int oid){
   String sql = "SELECT * FROM a2.ocean WHERE oid = ?;";
   try{
    ps = connection.prepareStatement(sql);
    ps.setString(1, oid);
    rs = ps.executeQuery();
    rs.next();
    return String.format("%d:%s:%d",rs.getInt(1),rs.getString(2),rs.getInt(3));
   } catch ((SQLException e){
    return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
  String sql = "UPDATE a2.HDI SET hdi_score =? WHERE cid = ? and year = ?;";
  try{
	ps = connection.prepareStatement(sql);
	ps.setInt(1, newHDI);
	ps.setInt(2, cid);
	ps.setInt(3, year);
	if(ps.executeUpdate() > 0){
	  return true;
	}
      } catch ((SQLException e){
	 return false;
      }
  
   return false;
  }

  public boolean deleteNeighbour(int c1id, c2id){
  String sql = "DELETE FROM a2.neighbour WHERE ((country = ? AND cuntry = ?) AND (neighbor = ? OR neighbor = ?));";
  try{
	ps = connection.prepareStatement(sql);
	ps.setInt(1, c1id);
	ps.setInt(2, c2id);
	ps.setInt(3, c1id);
	ps.setInt(4, c2id);
	if(ps.executeUpdate() > 0){
	  return true;
	}
      } catch ((SQLException e){
	 return false;
      }
   return false;
  }
  
  public String listCountryLanguages(int cid){
  String sql = "SELECT lid,lname,(lpercentage*population) AS lang_pop"
     + "FROM a2.language,a2.country WHERE country.cid=language.cid AND country.cid = ?"
    +"GROUP BY lid,lname,population,lpercentage ORDER BY population DESC;";
  try{
    ps = connection.prepareStatement(sql);
    ps.setInt(1, cid);
    rs = ps.executeQuery();
    String result = "";
    while (rs.next()){
      result += String.format("%s:%s:%s#",rs.getString(1),rs.getString(2),rs.getString(3));
    }
    return result;
    } catch (SQLException e){
      return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
  String sql = "UPDATE a2.country SET height = height - ? WHERE cid = ?;";
  try{
	ps = connection.prepareStatement(sql);
	ps.setInt(1, decrH);
	ps.setInt(2, cid);
	if (ps.executeUpdate() > 0){
	  return true;
	}
     } catch ((SQLException e){
      return false;
     }
    return false;
  }
    
  public boolean updateDB(){
	String query = "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE;CREATE TABLE a2.mostPopulousCountries(cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL);"; 
	String sql = "SELECT cid, cname FROM a2.country WHERE population > 100,000,000);";
	try{
		int count = 0;
		int insertions = 0;
		ps = connection.prepareStatement(sql);
		rs = ps.executeQuery();
		ps = connection.prepareStatement(query);
		if (ps.executeUpdate() > 0){
		  while (rs.next()){
		    count++;
		    String result = String.format("INSERT INTO mostPopulousCountries(cid, cname) VALUES(%s, %s);", rs.getString(1), rs.getString(2));
		    ps = connection.prepareStatement(result);
		    insertions += ps.executeUpdate();
		  }
		  if (count == insertions){
		    return true;
		  }
		}	
	} catch (SQLException e){
	} 
	return false;
  }
}
