import java.sql.*;
import java.util.ArrayList;

public class Assignment2 {
    
  // A connection to the database  
  private Connection connection;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // String for the statements
  String queryStatement;
  
  // Resultset for the query
  ResultSet rs;
  
  /**
   * Constructor for Assignment2
   */
  public Assignment2() {
	  try {
			Class.forName("org.postgresql.Driver");
	  } catch (ClassNotFoundException e) {
			System.out.println("Failed to find the JDBC driver");
	  }
	  
  }
  

  /**
   * Using the String input parameters which are the URL, username, and password
   * respectively, establish the Connection to be used for this session. Returns
   * true if the connection was successful.
   * 
   * @param URL: the url to connec to
   * @param username: username
   * @param password: password for username
   * @return true if connection was successful, false otherwise
   */
  public boolean connectDB(String URL, String username, String password){
	  try {
		  connection = DriverManager.getConnection(URL, username, password);
		  ps = connection.prepareStatement("set search_path to a2");
		  ps.execute();
	  } catch (SQLException e) {
		  return false;
	  }
	  if (connection == null) {
		  return false;
	  } else {
		  return true;
	  }
	  
  }
  
/**
 * Closes the connection.
 * 
 * @return True if closure was successful. False otherwise.
 */
  public boolean disconnectDB(){
  	if (this.connection == null) {
		return false;
	}
	try {
		this.connection.close();
		return true;
	} catch (SQLException e) {
		return false;
	}
	
  }
  
  /**
   * Inserts a row into the country table.
   * 
   * @param cid: id of the country
   * @param name: name of the country
   * @param height: highest elevation point of the country
   * @param population: population of the country
   * @return True if insertion was successful, false otherwise
   */
  public boolean insertCountry (int cid, String name, int height, int population) {
	queryStatement = "INSERT INTO country VALUES (?,?,?,?)";
	try {
		ps = connection.prepareStatement(queryStatement);
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.executeUpdate();
		return true;
	} catch (SQLException e) {
		return false;
	}
	
  }
  
  /**
   * Returns the number of countries in table "oceanAccess" that are located 
   * next to the ocean with id oid.
   * 
   * @param oid: id of the ocean
   * @return -1 if an error occurs. Otherwise, the number specified above.
   */
  public int getCountriesNextToOceanCount(int oid) {
	  int count = 0;
	  queryStatement = "SELECT COUNT(cid) AS total FROM OceanAccess where oid=?";
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  // only 1 tuple result since I used an aggregate function to get the total count
		  if (rs.next()) {
			  count = rs.getInt("total");
		  }
		  rs.close();
		  return count;
	  } catch (SQLException e) {
		  return -1;
	  }  
	  
  }
   
  /**
   * Returns a string with the information of an ocean with id oid.
   * 
   * @param oid: id of the ocean
   * @return String that contains information about the ocean. Empty string if
   * 		  ocean with oid doesn't exist
   */
  public String getOceanInfo(int oid) {
	  // store necessary information into the following variables
	  int oceanOid;
	  String oceanName;
	  int oceanDepth;
	  String output;
	  queryStatement = "SELECT * FROM Ocean where oid=?";
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.setInt(1, oid);
		  rs = ps.executeQuery();
		  // should only return 1 tuple since oid is a primary key
		  if (rs.next()) {
			  oceanOid = rs.getInt("oid");
			  oceanName = rs.getString("oname");
			  oceanDepth = rs.getInt("depth");
			  output = oceanOid + ":" + oceanName + ":" +oceanDepth;
			  return output;
		  }
		  rs.close();
		  return "";
	  } catch (SQLException e) {
		  return "";
	  }
	  
  }

  /**
   * Changes the HDI value of the country cid for the year 'year' to the HDI
   * value suppled (newHDI).
   * 
   * @param cid: id of the country
   * @param year: year for the HDI value to be changed
   * @param newHDI: new HDI value
   * @return true if change was successful, false otherwise.
   */
  public boolean chgHDI(int cid, int year, float newHDI){
	  queryStatement = "UPDATE hdi SET hdi_score=? WHERE cid=? and year=?";
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  ps.executeUpdate();
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
	  
  }

  /**
   * Deletes neighboring relation between two countries.
   * 
   * @param c1id: cid of country
   * @param c2id: cid of neighbor of c1id
   * @return true if change was successful, false otherwise
   */
  public boolean deleteNeighbour(int c1id, int c2id){
	  queryStatement = "DELETE FROM Neighbour WHERE country=? AND neighbor=?";
	  // delete tuple from neighbour where country=c1id and neighbor=c2id
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.setInt(1, c1id);
		  ps.setInt(2, c2id);
		  ps.executeUpdate();
	  } catch (SQLException e) {
		  return false;
	  }
	  // must also delete tuple from neighbour where country=c2id and neighbor=c1id
	  queryStatement = "DELETE FROM Neighbour WHERE country=? AND neighbor=?";
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.setInt(1, c2id);
		  ps.setInt(2, c1id);
		  ps.executeUpdate();
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
	  
  }       
 
  
  /**
   * Returns a string with all the languages that are spoken in the country with id cid.
   * 
   * @param cid: id of the country
   * @return String containing information about (lid, lname, lpopulation)
   *          the languages spoken in the country
   */
  public String listCountryLanguages(int cid){
	  int count = 0;
	  
	  // store necessary information in the following arraylists
	  ArrayList<String> lid = new ArrayList<String>();
	  ArrayList<String> lname = new ArrayList<String>();
	  ArrayList<Integer> lpopulation = new ArrayList<Integer>();
	  String result = "";
	  
	  queryStatement = "select lid, lname, lpercentage*population as lpopulation " + 
	  				   "from language L join country C on L.cid=C.cid where C.cid=?";
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.setInt(1, cid);
		  rs = ps.executeQuery();
		  while (rs.next()) {
			  lid.add(rs.getString("lid"));
			  lname.add(rs.getString("lname"));
			  lpopulation.add(rs.getInt("lpopulation"));
			  count++;
		  }
		  int i;
		  // concatonate the results into a string
		  for (i = 0; i < count; i++) {
			  result += lid.get(i)+":"+lname.get(i)+":"+lpopulation.get(i);
			  if (i != (count - 1)) {
				  result += "#";
			  }
		  }
		  rs.close();
		  return result;
	  } catch (SQLException e) {
		  return "";
	  }
	  
  }
  
  /**
   * Decreases the height of the country with id cid
   * 
   * @param cid: id of the country
   * @param decrH: amount to decrement the height of the country by
   * @return true if update was successful, false otherwise
   */
  public boolean updateHeight(int cid, int decrH){
	  queryStatement = "UPDATE country SET height=height-? WHERE cid=?";
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.setInt(1, decrH);
		  ps.setInt(2, cid);
		  ps.executeUpdate();
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
	  
  }
 /**
  * Create a table containing all the countries which have a population over 100 million.
  * Name of the table is mostPopulousCountries with attributes cid and cname.
  * 
  * @return true if the database was successfully update, false otherwise
  */
  public boolean updateDB(){
	  // create the table and insert into the database
	  queryStatement = "CREATE TABLE mostPopulousCountries( " +
	  				   "cid INTEGER REFERENCES country(cid) ON DELETE RESTRICT, " +
	  				   "cname VARCHAR(20) NOT NULL, " + 
	  				   "PRIMARY KEY(cid), " +
	  				   "FOREIGN KEY(cid) REFERENCES country(cid));";
	  try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.executeUpdate();
	  } catch (SQLException e) {
		  return false;
	  }
	  // population the new table with tuples that satisfy the condition
	  queryStatement = "INSERT INTO mostPopulousCountries (" + 
					   "SELECT cid, cname FROM country " +
					   "WHERE population>100000000 ORDER BY cid ASC);";
	   try {
		  ps = connection.prepareStatement(queryStatement);
		  ps.executeUpdate();
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }
	  
  }
}
  
