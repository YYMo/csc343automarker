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
	  try {
		  Class.forName("org.postgresql.Driver");
	  } catch (ClassNotFoundException e) {
		  System.out.println("PostgreSQL JDBC Driver must be included in path");
		  e.printStackTrace();
		  return;
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
		  connection = DriverManager.getConnection(URL, username, password);
	  } catch (SQLException e) {
		  return false;
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
		  return false;
	  }
	  return true;    
  }

  public boolean insertCountry (int cid, String name, int height, int population) {
   	try {
		String sqlText, rsText;
		sql = connection.createStatement();
		rsText = "SELECT cid FROM a2.country WHERE cid = " + cid;
   		rs = sql.executeQuery(rsText);
   		if (! rs.next()) {
			sqlText = "INSERT INTO a2.country VALUES (" + cid + ", '" + name + "', " + height + ", " + population +");";
   			sql.executeUpdate(sqlText);
   			sql.close();
   			rs.close();
   			return true;
		} else {
			sql.close();
			rs.close();
			return false;
		}
	} catch (SQLException e) {
		return false;
	}
  }

  public int getCountriesNextToOceanCount(int oid) {
	try {
		String sqlText;
		sqlText = "SELECT count(cid) FROM a2.OceanAccess where oid= " + oid + ";";
		sql = connection.createStatement();
		rs = sql.executeQuery(sqlText);
		if (rs != null) {
			while (rs.next()){
				return rs.getInt(1);
			}
		}
		rs.close();
		return -1;
	} catch (SQLException e) {
		return -1;
	}
  }

  public String getOceanInfo(int oid){
   try {
	   String sqlText, ocInfo;
	   sqlText = "SELECT oid, oname, depth FROM a2.ocean WHERE oid = " + oid + ";";
	   sql = connection.createStatement();
	   rs = sql.executeQuery(sqlText);
	   if (rs.next()){
		   ocInfo = rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
		   sql.close();
		   rs.close();
		   return ocInfo;
	   } else {
		   sql.close();
		   rs.close();
		   return "";
	   }
   } catch (SQLException e) {
	   return ""; 
   }	  
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   try {
        String sqlText;
        int changed;
        sql = connection.createStatement();
        rs = sql.executeQuery("SELECT cid FROM a2.hdi WHERE cid = " + cid + " AND year = " + year + ";");
        if(rs.next()) {
	        sqlText = "UPDATE a2.hdi SET hdi_score = " + newHDI + " WHERE cid = " + cid + " AND year = " + year + ";";
	        sql = connection.createStatement();
	        changed = sql.executeUpdate(sqlText);
	        sql.close();
	        if (changed > 0) {
	            return true;
	        } else {
	            return false;
	        }
         } else {
        	return false;
         }
    } catch (SQLException e) {
        return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try {
          String delete1, delete2;
          int changed1, changed2;
          sql = connection.createStatement();
	      sql = connection.createStatement();
	      delete1 = "DELETE FROM a2.neighbour WHERE country = " + c1id + " AND neighbor = " + c2id + ";";
	      changed1 = sql.executeUpdate(delete1);
	      delete2 = "DELETE FROM a2.neighbour WHERE country = " + c2id + " AND neighbor = " + c1id + ";";
	      changed2 = sql.executeUpdate(delete2);
          if(changed1 > 0 && changed2 > 0){
              return true;
          } else {
              return false;
          }
      } catch (SQLException e) {
          return false;
      }
  }

  public String listCountryLanguages(int cid){
	  try {
          String sqlText, sqlText2;
          String langlist = "";
          boolean exists = false;
          sql = connection.createStatement();
          sqlText = "SELECT cid FROM a2.language WHERE cid = " + cid;
          rs = sql.executeQuery(sqlText);
          if(rs.next()){
        	  exists = true;
          }
          rs.close();
          if (exists) {
              sql = connection.createStatement();
              sqlText2 = "SELECT l.lid, l.lname, (c.population * l.lpercentage) as population FROM a2.country c JOIN a2.language l on c.cid=l.cid WHERE c.cid = "
              	+ cid + " ORDER BY population ASC;";
              rs = sql.executeQuery(sqlText2);
              if(rs.next()){
                  do{
                      langlist += rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3) + "#";
                  } while(rs.next());
                  langlist = langlist.substring(0,langlist.length()-1);
              } else {
                  sql.close();
                  rs.close();
                  return "";
              }
              sql.close();
              rs.close();
              System.out.println(langlist);
              return langlist;
          }
          else{
              return "";
          }
      } catch (SQLException e) {
          return "";
      }
  }

 
  public boolean updateHeight(int cid, int decrH){
      try {
          String sqlText, newHeight;
          int changed = 0;
          sql = connection.createStatement();
          sqlText = "SELECT cid FROM a2.country WHERE cid = " + cid;
          rs = sql.executeQuery(sqlText);
          if (rs != null){
			  sqlText = "UPDATE a2.country SET height = " + decrH + " WHERE cid = " + cid + ";";
        	  changed = sql.executeUpdate(sqlText);
        	  rs.close();
              sql.close();
          } else {
          	  rs.close();
              sql.close();
              return false;
          }
          if(changed > 0) {
              return true;
          } else {
              return false;
          }
      } catch (SQLException e) {
          return false;
      }
  }
 
  public boolean updateDB(){
	try {
          String sqlText;
          List<Integer> cids = new ArrayList<Integer>();
          List<String> cnames = new ArrayList<String>();
          sql = connection.createStatement();
          sql.execute("CREATE TABLE a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20));");
          System.out.println("HSDJLSDFL");
          sqlText = "INSERT INTO a2.mostPopulousCountries (cid, cname) SELECT cid, cname from a2.country where population > 1000000;"; 
          rs = sql.executeQuery(sqlText);
          if (rs != null){
          	rs.close();
          	sql.close();	
          	return true;
		  } else {
			  rs.close();
          	  sql.close();
          	  return false;
		  }
      } catch (SQLException e) {
          return false;
      }   
  }
  
}

