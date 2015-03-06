import java.sql.*;
import java.io.*;

public class Assignment2 {
/**
//FOR DEBUGGING
    public static void main(String args[]) throws IOException, SQLException{
        int i;
        String s;
        Assignment2 a2 = new Assignment2();

        a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-c4wongjb", "c4wongjb", "");
        System.out.println("InsertCountry: " + a2.insertCountry(99, "XXX", 123, 999999999));
		System.out.println("getCountriesNext: " + a2.getCountriesNextToOceanCount(1));
		System.out.println("getOceanInfo: " + a2.getOceanInfo(1));
		System.out.println("chgHDI: " + a2.chgHDI(3, 2008, 9.99f));
		System.out.println("deleteNeighbour: " + a2.deleteNeighbour(1, 2));
		System.out.println("listCountryLanguages: " + a2.listCountryLanguages(1));
		System.out.println("updateHeight: " + a2.updateHeight(1, 40));
		System.out.println("updateDB: " + a2.updateDB());
		a2.disconnectDB();
  }
**/


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
			// System.out.println("not working");
        }
  }

  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
        try{
            connection = DriverManager.getConnection(URL, username, "");
        }catch (SQLException e) {
            return false;
        }
        return true;
  }


  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
		if (connection == null) {
			return false;
		}
        try{
            connection.close();
        } catch (SQLException e) {
            return false;
        }
        return true;
  }

  public boolean insertCountry (int cid, String name, int height, int population) throws SQLException{
      try {
          ps = connection.prepareStatement(
                         "INSERT INTO A2.COUNTRY(cid, cname, height, population) Values(?, ?, ?, ?)");
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4,population);
          ps.executeUpdate();
		  
		  ps.close();
      } catch (SQLException e){
          return false;
	}
	return true;
  }



  public int getCountriesNextToOceanCount(int oid) throws SQLException{
      int i = -1;
      try {
          sql = connection.createStatement();
          String sqlText;
          sqlText = "SELECT COUNT(cid) FROM a2.oceanaccess WHERE oid=" + oid;
          rs = sql.executeQuery(sqlText);
          if (rs.next()) {
              i = rs.getInt(1);
          }
		  rs.close();
		  sql.close();
      } catch (SQLException e){
			// System.err.println("SQL Exception.<Message>:" + e.getMessage());
      }
      return i;
  }




  public String getOceanInfo(int oid) throws SQLException{
      String s = "";
      try {
          sql = connection.createStatement();
          String sqlText;
          sqlText = "SELECT * FROM A2.ocean WHERE oid=" + oid;
          rs = sql.executeQuery(sqlText);
          if (rs.next()) {
              s = rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3);
          }
          rs.close();
		  sql.close();
      } catch (SQLException e) {
		// System.err.println("SQL Exception.<Message>:" + e.getMessage());
      }
      return s;
  }


  public boolean chgHDI(int cid, int year, float newHDI) {
	  boolean success = false;
      try {
          sql = connection.createStatement();
          String sqlText;
		  sqlText = "SELECT * FROM A2.HDI WHERE CID=" + cid + " AND YEAR=" + year;
          rs = sql.executeQuery(sqlText);
          if (rs.next()) {
              success = true;
			  sqlText = "UPDATE A2.HDI SET HDI_SCORE=" + newHDI + " WHERE CID=" + cid + " AND YEAR=" + year;
			  sql.executeUpdate(sqlText);
          }
		  rs.close();
		  sql.close();
		  return success;
      } catch (SQLException e) {
		// System.err.println("SQL Exception.<Message>:" + e.getMessage());
        return false;
      }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
		  sql = connection.createStatement();
		  String sqlText = "DELETE FROM a2.Neighbour AS N";
		  sqlText = sqlText + " WHERE (N.country=" + Integer.toString(c1id) + " AND N.neighbor=" + Integer.toString(c2id) + " )";
		  sqlText = sqlText + " OR (N.country=" + Integer.toString(c2id) + " AND N.neighbor=" + Integer.toString(c1id) + " )";
		  sql.executeUpdate(sqlText);
		  sql.close();
		  return true;
	  }
	  catch (SQLException e) {
			// System.err.println("SQL Exception.<Message>:" + e.getMessage());
		return false;
	  }   
  }
  
  public String listCountryLanguages(int cid){
	String sqlText = "SELECT L.lid, L.lname, C.population*L.lpercentage";
	sqlText = sqlText + " FROM a2.Country AS C, a2.Language AS L";
	sqlText = sqlText + " WHERE C.cid=L.cid AND C.cid=" + Integer.toString(cid);
	sqlText = sqlText + " ORDER BY population";
	String s = "";
	try {
		sql = connection.createStatement();
		ResultSet rs = sql.executeQuery(sqlText);
		if (rs.next()) {
			s = s + rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
		}
		while (rs.next()) {
			s = s + "#" + rs.getString(1) + ":" + rs.getString(2) + ":" + rs.getString(3);
		}
		
		rs.close();
		sql.close();
	} catch (SQLException e) {
		// System.err.println("SQL Exception.<Message>:" + e.getMessage());
		return "";
	}
	return s;
  }
 
  public boolean updateHeight(int cid, int decrH){
	String sqlText = "SELECT height FROM a2.Country WHERE cid=" + cid;
	try {
		sql = connection.createStatement();
		ResultSet rs = sql.executeQuery(sqlText);
		if (rs.next()) {
			int h = rs.getInt(1);
			sql.executeUpdate("UPDATE a2.Country SET height=" + (h-decrH) + " WHERE cid=" + cid);
			rs.close();
			sql.close();
			return true;
		}
	} catch (SQLException e) {
		// System.err.println("SQL Exception.<Message>:" + e.getMessage());
		return false;
	}
	return false;
  }
  
  public boolean updateDB(){
	try {
	  	sql = connection.createStatement();
		sql.executeUpdate("DROP TABLE a2.mostPopularCountries");
		sql.close();
	}
	catch (SQLException e) {
	}

	try {
		sql = connection.createStatement();
		String sqlText = "CREATE TABLE a2.mostPopularCountries(";
		sqlText = sqlText + " cid int, cname varchar(20) )";
		sql.executeUpdate(sqlText);
			
		sqlText = "INSERT INTO a2.mostPopularCountries (";
		sqlText = sqlText + "SELECT cid, cname FROM a2.Country WHERE population>100000000 )";
		sql.executeUpdate(sqlText);
		sql.close();
		return true;
	}
	catch (SQLException e2) {
			// System.err.println("SQL Exception.<Message>:" + e.getMessage());
		return false;
	}
  }
}