import java.sql.*;

public class Assignment2 {
	
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // ResultSet for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	  try {
		Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException e) {
		e.printStackTrace();
	}
  }
  
  //Using the input parameters, establish a connection to be used for 
  //this session. Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password){
	  try {
		connection = DriverManager.getConnection(
				  URL, username , password);
		return true;
	} catch (SQLException e) {
		return false;
	}
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
	  try {
		  rs.close(); 
		  ps.close(); 
		  connection.close();
		  return true;
		  } catch (SQLException e) {
			  return false;
		  } 
  }
    
  public boolean insertCountry (int cid, String name, 
		  int height, int population) {
   try {
	   	ps = connection.prepareStatement(
	   		"INSERT INTO a2.Country(cid, cname, height, population) " +
	   		"VALUES(?, ?, ?, ?);");
	   	ps.setInt(1, cid);
	   	ps.setString(2, name);
	   	ps.setInt(3, height);
	   	ps.setInt(4, population);
	   	ps.executeUpdate();
	   	ps.close();
	   	return true;
   } catch (SQLException e) {
	   return false;
   }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  try {
		ps = connection.prepareStatement(
				  "SELECT count(DISTINCT cid) AS numCountries " +
				  "FROM a2.oceanAccess " +
				  "WHERE oid = ?;");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		rs.next();
		int result = rs.getInt("numCountries");
		if (result == 0) {
			result = -1;
		}
		rs.close();
		return result;
	} catch (SQLException e) {
		return -1;
	}
  }
   
  public String getOceanInfo(int oid){
	  try {
		ps = connection.prepareStatement(
				  "SELECT oid, oname, depth " +
				  "FROM a2.ocean " +
				  "WHERE oid = ?;");
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		rs.next();
		String returnStr = rs.getInt("oid") + ":" + rs.getString("oname") + 
			":" + rs.getInt("depth");
		rs.close();
		return returnStr;
	} catch (SQLException e) {
		return "";
	}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try {
		//Retrieve relevant row of data
		ps = connection.prepareStatement(
				  "SELECT cid, year, hdi_score " +
				  "FROM a2.hdi " +
				  "WHERE cid = ? AND year = ?;");
		ps.setInt(1, cid);
		ps.setInt(2, year);
		rs = ps.executeQuery();
		rs.next();
		int[] returnVals = new int[2];
		returnVals[0] = rs.getInt("cid");
		returnVals[1] = rs.getInt("year");
		//Delete relevant row of data
		ps = connection.prepareStatement(
				"DELETE FROM a2.hdi " +
				"WHERE cid = ? AND year = ?;");
		ps.setInt(1, returnVals[0]);
		ps.setInt(2, returnVals[1]);
		ps.executeUpdate();
		//Insert altered row of data
		ps = connection.prepareStatement(
				"INSERT INTO a2.hdi(cid, year, hdi_score) " +
				"VALUES(?, ?, ?);");
		ps.setInt(1, returnVals[0]);
		ps.setInt(2, returnVals[1]);
		ps.setFloat(3, newHDI);
		ps.executeUpdate();
		ps.close();
		rs.close();
		return true;
	} catch (SQLException e) {
		return false;
	}
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
		//Delete first relation
		ps = connection.prepareStatement(
				  "DELETE FROM a2.neighbour " +
				  "WHERE country = ? AND neighbor = ?;");
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		ps.executeUpdate();
		//Delete second relation
		ps = connection.prepareStatement(
				  "DELETE FROM a2.neighbour " +
				  "WHERE country = ? AND neighbor = ?;");
		ps.setInt(1, c2id);
		ps.setInt(2, c1id);
		ps.executeUpdate();
		ps.close();
		return true;
	} catch (SQLException e) {
		return false;
	}     
  }
  
  public String listCountryLanguages(int cid){
	  try {
		ps = connection.prepareStatement(
			"SELECT language.lid AS lid, language.lname AS lname, " +
				 "(lpercentage * country.population) AS lpopulation " +
			"FROM a2.language, a2.country " +
			"WHERE language.cid = country.cid AND language.cid = ?;");
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		String returnStr = "";
		int i = 1;
		while(rs.next()) {
			int lid = rs.getInt("lid");
			String lname = rs.getString("lname");
			int lpop = rs.getInt("lpopulation");
			returnStr = returnStr + lid + ":" + lname + ":" + lpop + "#";
			i++;
		}
		if (!returnStr.isEmpty()) {
			returnStr = returnStr.substring(0, returnStr.length() - 1);
		}
		ps.close();
		rs.close();
		return returnStr;
	} catch (SQLException e) {
		return "";
	}
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try {
			//Retrieve relevant row of data
			ps = connection.prepareStatement(
					  "SELECT cid, cname, height, population " +
					  "FROM a2.country " +
					  "WHERE cid = ?;");
			ps.setInt(1, cid);
			rs = ps.executeQuery();
			Object[] returnVals = new Object[4];
			rs.next();
			returnVals[0] = rs.getInt("cid");
			returnVals[1] = rs.getString("cname");
			returnVals[2] = rs.getInt("height");
			returnVals[2] = (Integer) returnVals[2] - decrH;
			returnVals[3] = rs.getInt("population");
			//Delete relevant row of data
			ps = connection.prepareStatement(
					"DELETE FROM a2.country " +
					"WHERE cid = ?;");
			ps.setInt(1, (Integer) returnVals[0]);
			ps.executeUpdate();
			//Insert altered row of data
			ps = connection.prepareStatement(
					"INSERT INTO a2.country(cid, cname, height, population) " +
					"VALUES(?, ?, ?, ?);");
			ps.setInt(1, (Integer) returnVals[0]);
			ps.setString(2, (String) returnVals[1]);
			ps.setInt(3, (Integer) returnVals[2]);
			ps.setInt(4, (Integer) returnVals[3]);
			ps.executeUpdate();
			rs.close();
			ps.close();
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
  }
    
  public boolean updateDB(){
	 try {
		ps = connection.prepareStatement(
				"CREATE TABLE a2.mostPopulousCountries(" +
				"cid INTEGER, " +
				"cname VARCHAR(20));");
		ps.executeUpdate();
		ps = connection.prepareStatement(
				"SELECT cid, cname " +
				"FROM a2.country " +
				"WHERE population > 100000000 " +
				"ORDER BY cid ASC;");
		rs = ps.executeQuery();
		while (rs.next()) {
			ps = connection.prepareStatement(
					"INSERT INTO a2.mostPopulousCountries(cid, cname) " +
					"VALUES(?, ?);");
			ps.setInt(1, rs.getInt("cid"));
			ps.setString(2, rs.getString("cname"));
			ps.executeUpdate();
		}
		ps.close();
		rs.close();
		return true;
	} catch (SQLException e) {
		e.printStackTrace();
		return false;
	}
  }
  
}
