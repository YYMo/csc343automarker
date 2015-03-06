import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Result set for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	  try {
		  Class.forName("org.postgresql.Driver");
	  } catch (ClassNotFoundException e){
		  e.printStackTrace();	//it should not send output.	  
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password){
	  try {
		connection = DriverManager.getConnection(URL, username, password);
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
    return true;
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
	  try {
		connection.close();
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
    return true;    
  }
    
  /*
   * Inserts a row into the country table. cid is the name of the country, 
   * name is the name of the country, height is the highest elevation point,
   * and population is the population of the country.
  **/
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
		//Create a statement for executing SQL queries.
		sql = connection.createStatement();
		//Check if cid already exists.
		String checkExistance;
		checkExistance = "SELECT cid " + 
						"FROM country " + 
						"WHERE cid = ? ";
		PreparedStatement ps = connection.prepareStatement(checkExistance);
		// Insert the parameter into the PreparedStatement and execute it.
		ps.setString(1, Integer.toString(cid));
		rs = sql.executeQuery(checkExistance);
		// The country with cid already exists, therefore it can not insert it.
		if (rs != null){
			return false;
		} else { //the country with cid does not exist.
			//insert into the country table
			String sqlText;
			sqlText = "INSERT INTO country " + "VALUES ( ?, '?', ?, ? ) ";
			PreparedStatement ps2 = connection.prepareStatement(sqlText);
			// Insert the parameters into the PreparedStatement and execute it.
			ps2.setString(1, Integer.toString(cid));
			ps2.setString(2, name);
			ps2.setString(3, Integer.toString(height));
			ps2.setString(4, Integer.toString(population));
			sql.executeUpdate(sqlText);
			//Check if the update did occur.
			int update = sql.getUpdateCount();
			if(update < 0 ){
				return false;
			}
		}
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
    return true;
  }
  
  /*
   * Returns the number of countries in table "oceanAccess" that are located next to
   * ocean with oid.
   */
  public int getCountriesNextToOceanCount(int oid) {
	  int numOfCountries = 0; //number of countries next to Ocean with this oid.
	  try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT COUNT(cid) AS numOfCountries " +
				"FROM oceanAccess " + 
				"WHERE oid = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Integer.toString(oid));
		rs = sql.executeQuery(sqlText);
		if(rs == null){
			return -1;
		} else {
			// Iterate through the result set and report the number of countries located next to oid.
			while (rs.next()) { 
				numOfCountries = rs.getInt("numOfCountries");
			}
		}		
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return -1;
	  }
	  return numOfCountries;
  }
   
  /*
   *Returns a string with the information of an ocean with oid.
   *Output format is: "oid:oname:depth"
   *Return empty string "" if the oid does not exist. 
   **/
  public String getOceanInfo(int oid){
	  int oceanID = 0;
	  String oName = null;
	  int oDepth = 0;
	  try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT * " +
				"FROM ocean " + 
				"WHERE oid = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Integer.toString(oid));
		rs = sql.executeQuery(sqlText);
		//if oid does not exist then rs will be null.
		if(rs == null){
			return "";
		} else { // Iterate through the result set and report the info from this oid
			while (rs.next()) { 
				oceanID = rs.getInt("oid");
				oName = rs.getString("oname");
				oDepth = rs.getInt("depth");
			}
		}		
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return "";
	  }
	  return Integer.toString(oceanID) + ":" + oName + ":" + Integer.toString(oDepth);
  }

  /*
   *Changes the HDI value of a country cid for the year to the new HDI
   *Return true if change was successful.
   **/
  public boolean chgHDI(int cid, int year, float newHDI){
	try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "UPDATE hdi " +
				"SET hdi_score = ? " + 
				"WHERE cid = ? AND year = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Float.toString(newHDI));
		ps.setString(2, Integer.toString(cid));
		ps.setString(3, Integer.toString(year));
		sql.executeUpdate(sqlText);
		//Check if the update did occur.
		int update = sql.getUpdateCount();
		if(update < 0 ){
			return false;
		}
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	  }
	return true;
  }

  /*
   *Deletes the neighbouring relation between two countries.
   *Returns true if the deletion was successful.
   **/
  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
			sql = connection.createStatement();
			String sqlText;
			sqlText = "DELETE " +
					"FROM neighbour " + 
					"WHERE country = ? AND neighbor = ? " + 
					"OR country = ? AND neighbor = ? ";
			PreparedStatement ps = connection.prepareStatement(sqlText);
			ps.setString(1, Integer.toString(c1id));
			ps.setString(2, Integer.toString(c2id));
			ps.setString(3, Integer.toString(c2id));
			ps.setString(4, Integer.toString(c1id));
			sql.executeUpdate(sqlText);
			//Check if the update did occur.
			int update = sql.getUpdateCount();
			if(update < 0 ){
				return false;
			}
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return false;
	  }
	  return true;        
  }
  
  /*
   *Returns a string with all the languages that are 
   *spoken in the country with this cid ordered by population.
   *In this format: "l1id:l1lname:l1popultation#l2id:l2lname:l2population#...." id, name, population
   *Return empty "" if the country does not exist.
   **/
  public String listCountryLanguages(int cid){
	  int population = 0;
	  String resultString = null;
	  try {
			sql = connection.createStatement();
			String countryPopulation;
			countryPopulation = "SELECT population " +
								"FROM country " + 
								"WHERE cid = ? ";
			PreparedStatement ps = connection.prepareStatement(countryPopulation);
			ps.setString(1, Integer.toString(cid));
			rs = sql.executeQuery(countryPopulation);
			//if cid does not exist then rs will be null.
			if(rs == null){
				return "";
			} else { // Iterate through the result set and get the population of the country with this cid.
				while (rs.next()) { 
					population = rs.getInt("population");
				}
			}
			//Use language table and population to get desired info.
			String sqlText = "SELECT lid, lname, lpercentage*? AS languagepopulation " + 
							"FROM language " + 
							"WHERE cid = ? " +
							"ORDER BY languagepopulation ASC ";
			PreparedStatement ps2 = connection.prepareStatement(sqlText);
			ps2.setString(1, Integer.toString(population));
			ps2.setString(2, Integer.toString(cid));
			rs = sql.executeQuery(sqlText);
			if(rs == null){
				return "";
			} else { // Iterate through the result set and get the lid, lname and lpercentage * population.
				while (rs.next()) { 
					resultString = rs.getString("lid") + ":";
					resultString += rs.getString("lname") + ":";
					resultString += rs.getString("languagepopulation") + "#";
				}
			}
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return "";
	  }
	  return resultString;
  }
  
  /*
   * Decrease the height of the country with id cid.
   * Returns true if the update was successful.
   **/
  public boolean updateHeight(int cid, int decrH){
	  try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "UPDATE country " + 
				"SET height = ? " +
				" WHERE cid = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Integer.toString(decrH));
		ps.setString(2, Integer.toString(cid));
		sql.executeUpdate(sqlText);
		//Check if the update did occur.
		int update = sql.getUpdateCount();
		if(update < 0 ){
			return false;
		}
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
	return true;
  }
    
  /*
   *Creates a table called mostPopulousContries which contains all the 
   *countries which have a population over 100 million.
   *Its attributes are cid INTEGER(country id) and cname VARCHARD(20) (country name).
   *Its sorted in ascending order by cids.
   *Returns true if the database was successfully updated.
   **/
  public boolean updateDB(){
	  try {
		  sql = connection.createStatement();
		  String sqlText;
		  sqlText = "SELECT cid, cname " +
				  "INTO mostPopulousContries " +
				  "FROM country " + 
				  "WHERE population > 100000000 ";
		  sql.executeUpdate(sqlText);
		  //Check if the update did occur.
		  int update = sql.getUpdateCount();
		  if(update < 0 ){
			  return false;
		  }
		} catch (SQLException e) {
			//e.printStackTrace();
			return false;
		}
		return true;   
  }
}
import java.sql.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Result set for the query
  ResultSet rs;
  
  //CONSTRUCTOR
  Assignment2(){
	  try {
		  Class.forName("org.postgresql.Driver");
	  } catch (ClassNotFoundException e){
		  e.printStackTrace();	//TODO: it should not send output.	  
	  }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is successful
  public boolean connectDB(String URL, String username, String password){
	  try {
		connection = DriverManager.getConnection(URL, username, password);
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
    return true;
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
	  try {
		connection.close();
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
    return true;    
  }
    
  /*
   * Inserts a row into the country table. cid is the name of the country, 
   * name is the name of the country, height is the highest elevation point,
   * and population is the population of the country.
  **/
  public boolean insertCountry (int cid, String name, int height, int population) {
	  try {
		//Create a statement for executing SQL queries.
		sql = connection.createStatement();
		//Check if cid already exists.
		String checkExistance;
		checkExistance = "SELECT cid " + 
						"FROM country " + 
						"WHERE cid = ? ";
		PreparedStatement ps = connection.prepareStatement(checkExistance);
		// Insert the parameter into the PreparedStatement and execute it.
		ps.setString(1, Integer.toString(cid));
		rs = sql.executeQuery(checkExistance);
		// The country with cid already exists, therefore it can not insert it.
		if (rs != null){
			return false;
		} else { //the country with cid does not exist.
			//insert into the country table
			String sqlText;
			sqlText = "INSERT INTO country " + "VALUES ( ?, '?', ?, ? ) ";
			PreparedStatement ps2 = connection.prepareStatement(sqlText);
			// Insert the parameters into the PreparedStatement and execute it.
			ps2.setString(1, Integer.toString(cid));
			ps2.setString(2, name);
			ps2.setString(3, Integer.toString(height));
			ps2.setString(4, Integer.toString(population));
			sql.executeUpdate(sqlText);
			//Check if the update did occur.
			int update = sql.getUpdateCount();
			if(update < 0 ){
				return false;
			}
		}
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
    return true;
  }
  
  /*
   * Returns the number of countries in table "oceanAccess" that are located next to
   * ocean with oid.
   */
  public int getCountriesNextToOceanCount(int oid) {
	  int numOfCountries = 0; //number of countries next to Ocean with this oid.
	  try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT COUNT(cid) AS numOfCountries " +
				"FROM oceanAccess " + 
				"WHERE oid = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Integer.toString(oid));
		rs = sql.executeQuery(sqlText);
		if(rs == null){
			return -1;
		} else {
			// Iterate through the result set and report the number of countries located next to oid.
			while (rs.next()) { 
				numOfCountries = rs.getInt("numOfCountries");
			}
		}		
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return -1;
	  }
	  return numOfCountries;
  }
   
  /*
   *Returns a string with the information of an ocean with oid.
   *Output format is: "oid:oname:depth"
   *Return empty string "" if the oid does not exist. 
   **/
  public String getOceanInfo(int oid){
	  int oceanID = 0;
	  String oName = null;
	  int oDepth = 0;
	  try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "SELECT * " +
				"FROM ocean " + 
				"WHERE oid = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Integer.toString(oid));
		rs = sql.executeQuery(sqlText);
		//if oid does not exist then rs will be null.
		if(rs == null){
			return "";
		} else { // Iterate through the result set and report the info from this oid
			while (rs.next()) { 
				oceanID = rs.getInt("oid");
				oName = rs.getString("oname");
				oDepth = rs.getInt("depth");
			}
		}		
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return "";
	  }
	  return Integer.toString(oceanID) + ":" + oName + ":" + Integer.toString(oDepth);
  }

  /*
   *Changes the HDI value of a country cid for the year to the new HDI
   *Return true if change was successful.
   **/
  public boolean chgHDI(int cid, int year, float newHDI){
	try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "UPDATE hdi " +
				"SET hdi_score = ? " + 
				"WHERE cid = ? AND year = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Float.toString(newHDI));
		ps.setString(2, Integer.toString(cid));
		ps.setString(3, Integer.toString(year));
		sql.executeUpdate(sqlText);
		//Check if the update did occur.
		int update = sql.getUpdateCount();
		if(update < 0 ){
			return false;
		}
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	  }
	return true;
  }

  /*
   *Deletes the neighbouring relation between two countries.
   *Returns true if the deletion was successful.
   **/
  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
			sql = connection.createStatement();
			String sqlText;
			sqlText = "DELETE " +
					"FROM neighbour " + 
					"WHERE country = ? AND neighbor = ? " + 
					"OR country = ? AND neighbor = ? ";
			PreparedStatement ps = connection.prepareStatement(sqlText);
			ps.setString(1, Integer.toString(c1id));
			ps.setString(2, Integer.toString(c2id));
			ps.setString(3, Integer.toString(c2id));
			ps.setString(4, Integer.toString(c1id));
			sql.executeUpdate(sqlText);
			//Check if the update did occur.
			int update = sql.getUpdateCount();
			if(update < 0 ){
				return false;
			}
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return false;
	  }
	  return true;        
  }
  
  /*
   *Returns a string with all the languages that are 
   *spoken in the country with this cid ordered by population.
   *In this format: "l1id:l1lname:l1popultation#l2id:l2lname:l2population#...." id, name, population
   *Return empty "" if the country does not exist.
   **/
  public String listCountryLanguages(int cid){
	  int population = 0;
	  String resultString = null;
	  try {
			sql = connection.createStatement();
			String countryPopulation;
			countryPopulation = "SELECT population " +
								"FROM country " + 
								"WHERE cid = ? ";
			PreparedStatement ps = connection.prepareStatement(countryPopulation);
			ps.setString(1, Integer.toString(cid));
			rs = sql.executeQuery(countryPopulation);
			//if cid does not exist then rs will be null.
			if(rs == null){
				return "";
			} else { // Iterate through the result set and get the population of the country with this cid.
				while (rs.next()) { 
					population = rs.getInt("population");
				}
			}
			//Use language table and population to get desired info.
			String sqlText = "SELECT lid, lname, lpercentage*? AS languagepopulation " + 
							"FROM language " + 
							"WHERE cid = ? " +
							"ORDER BY languagepopulation ASC ";
			PreparedStatement ps2 = connection.prepareStatement(sqlText);
			ps2.setString(1, Integer.toString(population));
			ps2.setString(2, Integer.toString(cid));
			rs = sql.executeQuery(sqlText);
			if(rs == null){
				return "";
			} else { // Iterate through the result set and get the lid, lname and lpercentage * population.
				while (rs.next()) { 
					resultString = rs.getString("lid") + ":";
					resultString += rs.getString("lname") + ":";
					resultString += rs.getString("languagepopulation") + "#";
				}
			}
	  } catch (SQLException e) {
		  //e.printStackTrace();
		  return "";
	  }
	  return resultString;
  }
  
  /*
   * Decrease the height of the country with id cid.
   * Returns true if the update was successful.
   **/
  public boolean updateHeight(int cid, int decrH){
	  try {
		sql = connection.createStatement();
		String sqlText;
		sqlText = "UPDATE country " + 
				"SET height = ? " +
				" WHERE cid = ? ";
		PreparedStatement ps = connection.prepareStatement(sqlText);
		ps.setString(1, Integer.toString(decrH));
		ps.setString(2, Integer.toString(cid));
		sql.executeUpdate(sqlText);
		//Check if the update did occur.
		int update = sql.getUpdateCount();
		if(update < 0 ){
			return false;
		}
	} catch (SQLException e) {
		//e.printStackTrace();
		return false;
	}
	return true;
  }
    
  /*
   *Creates a table called mostPopulousContries which contains all the 
   *countries which have a population over 100 million.
   *Its attributes are cid INTEGER(country id) and cname VARCHARD(20) (country name).
   *Its sorted in ascending order by cids.
   *Returns true if the database was successfully updated.
   **/
  public boolean updateDB(){
	  try {
		  sql = connection.createStatement();
		  String sqlText;
		  sqlText = "SELECT cid, cname " +
				  "INTO mostPopulousContries " +
				  "FROM country " + 
				  "WHERE population > 100000000 ";
		  sql.executeUpdate(sqlText);
		  //Check if the update did occur.
		  int update = sql.getUpdateCount();
		  if(update < 0 ){
			  return false;
		  }
		} catch (SQLException e) {
			//e.printStackTrace();
			return false;
		}
		return true;   
  }
}
