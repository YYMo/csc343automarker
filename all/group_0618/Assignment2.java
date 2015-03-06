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
  //it's rainsing an exception if the driver is not found
  Assignment2() throws ClassNotFoundException{ 
	  Class.forName("org.postgresql.Driver"); 
  }
  
  //working
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection
				  (
						  URL, 
						  username, 
						  password
				  ); 
		  //String heroku = "jdbc:postgresql://ec2-54-204-39-187.compute-1.amazonaws.com:5432/d5a9oa31qd7980?user=mlokajknahavsr&password=og90o6I7xSoIeG7zu49NCgah33&ssl=true&sslfactory=org.postgresql.ssl.NonValidatingFactory";
		  //connection = DriverManager.getConnection(heroku);
	} catch (SQLException e) {
		return false;
	}
	  return true;
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
  
  //working
  public boolean insertCountry (int cid, String name, int height, int population) {
	  String sqlText = "insert into a2.country values (?, ?, ?, ?)";
	  try {
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		int nRows = ps.executeUpdate();
		if(nRows == 0)
			return false;
		else
			return true;
	} catch (SQLException e) {
		return false;
	}
  }
  
  //working
  public int getCountriesNextToOceanCount(int oid) {
	  String sqlText = "select count(cid) count from a2.oceanaccess where oid=?";
	  
	  try {
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, oid);
		
		rs = ps.executeQuery();
		
		int result = 0;
		
		while(rs.next())
		{
			result = rs.getInt(1);
		}
		
		if(result == 0)
			return -1;
		else
			return result;
	} catch (SQLException e) {
		return -2;
	}
	    
  }
  
  //working
  public String getOceanInfo(int oid){
	  String textQuery = "select oid, oname, depth from a2.ocean where oid = ? ";
	  try {
		ps = connection.prepareStatement(textQuery);
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		
		String oname = ""; 
		int depth = 0;
		String result = "";
		
		while(rs.next()) {
			oname = rs.getString("oname");
			depth = rs.getInt("depth");
			result += oid+":"+oname+":"+depth;
		}
		
		return result;
	} catch (SQLException e) {
		return "";
	}
  }

  //working
  public boolean chgHDI(int cid, int year, float newHDI){
	  String sqlText = "UPDATE a2.hdi SET hdi_score = ? where cid = ? and year = ?"; 
	  try {
		ps = connection.prepareStatement(sqlText);
		ps.setFloat(1, newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		int nRowsUpdated = ps.executeUpdate();
		
		if(nRowsUpdated < 1)
			return false;
	} catch (SQLException e) {
		return false;
	}
	  return true;
  }

  //working
  public boolean deleteNeighbour(int c1id, int c2id){
	  String sqlText = "DELETE FROM a2.neighbour where country = ? and neighbor = ?";
	  try {
		ps = connection.prepareStatement(sqlText);
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		int nRows = ps.executeUpdate();
		
		if(nRows == 0)
			return false;
		else return true;
	} catch (SQLException e) {
		return false;
	}
  }
  
  //working
  public String listCountryLanguages(int cid){
	  String textQuery = "select lid, lname, (lpercentage*country.population/100) population from a2.language, a2.country where language.cid = country.cid and country.cid = ? order by population";
	  try {
		ps = connection.prepareStatement(textQuery);
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		
		String lid, lname, population, result = "";
		int i = 0;
		while(rs.next()) {
			i++;
			lid = rs.getString("lid");
			lname = rs.getString("lname");
			population = rs.getString("population"); //posso tratar como string?
			result += "|"+i+"id:" + lid + "|"+i+"name:"+ lname + "|"+i+"population#" + population;
		}
		
		return result;
	} catch (SQLException e) {
		return "";
	}
  }
  
  //working
  public boolean updateHeight(int cid, int decrH){
	  String sqlTextGet = "select height from a2.country where cid=?";
	  String sqlTextUpdate = "UPDATE a2.country SET height=? where cid=?";
	  
	  try {
		ps = connection.prepareStatement(sqlTextGet);
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		
		int height_db = decrH;
		while(rs.next()) {
			height_db = rs.getInt("height");
		}
		ps = connection.prepareStatement(sqlTextUpdate);
		ps.setInt(1, height_db-decrH);
		ps.setInt(2, cid);
		int result = ps.executeUpdate();
		if(result == 0) return false;
		else return true;
		
	} catch (SQLException e) {
		return false;
	}
  }
  
  public boolean updateDB(){
	  String sqlCreate = "CREATE TABLE a2.mostPopulousCountries (cid INTEGER PRIMARY KEY, cname VARCHAR(20) NOT NULL)";
	  String sqlPopulate = "insert into a2.mostPopulousCountries (select cid, cname from a2.country where population > 100*10e6 order by cid)";
	  try {
		ps = connection.prepareStatement(sqlCreate);
		
		int nRows = ps.executeUpdate();
		
		
		ps = connection.prepareStatement(sqlPopulate);
		nRows = ps.executeUpdate();
		if(nRows != 0) 
			return true;
		else
			return false;
		
		
	} catch (SQLException e) {
		return false;
	}  
  }
  
}
