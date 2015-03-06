import java.sql.*;
import java.io.*;

public class Assignment2 {
    
  // A connection to the database  
  Connection connection;
  
  // Statement to run queries
  Statement sql;
  
  // Prepared Statement
  PreparedStatement ps;
  
  // Resultset for the query
  ResultSet rs;
  
  // Identifies the postgreSQL driver using Class.forName method
   public Assignment2() throws ClassNotFoundException{
  
	try {
		Class.forName("org.postgresql.Driver");
    }
    catch (ClassNotFoundException e) {
		throw new ClassNotFoundException();
    }
  }
  
  /** 
   * Using the input parameters, establish a connection to be used for 
   * this session. Returns true if connection is sucessful.
   */
  public boolean connectDB(String URL, String username, String password) {
		try {
			connection = DriverManager.getConnection(URL, username, password); //using input parameters
		} catch (SQLException se) {
			return false; 
	    }
	  return true;
  }
  
  /**
   * Closes the connection. Returns true if closure was sucessful.
   */
  public boolean disconnectDB() {
	try {
		if (connection.isClosed()) {
			return false; //false if already closed
		} else {
			connection.close();
			return true;
		}
	} catch (SQLException se) { 
		return false;
	}
  }
  /**
   * Inserts a row into the country table. cid is the name of the country, 
   * name is the name of the country, height is the highest elevation point and
   * population is the population of the newly inserted country. You have to 
   * check if the country with id cid exists. Returns true if the insertion was 
   * successful, false otherwise.
   */
  public boolean insertCountry (int cid, String name, int height, int population) {
	try {
		ps = connection.prepareStatement("Insert into a2.country Values (?,?,?,?);"); //Uses a prepared statement to insert country values into the country table
		ps.setInt(1, cid);
		ps.setString(2, name);
		ps.setInt(3, height);
		ps.setInt(4, population);
		ps.executeUpdate();
	} catch (SQLException se) { 
		return false; //returns false if insertion failed
	}
	return true; //returns true for successful insertion
  }
  /**
   * Returns the number of countries in table oceanAccess that are located next 
   * to the ocean with id oid. Returns -1 if an error occurs.
   */
  public int getCountriesNextToOceanCount(int oid) {
	int num_countries = 0;
	try {
		sql = connection.createStatement(); //creating a connection
		rs = sql.executeQuery("select oid, count(*) as count from a2.oceanAccess group by oid;"); //SQL query to extract the correct data
		while (rs.next()) {
			if (oid == rs.getInt(1)) {
				num_countries = rs.getInt(2);
			}
		}
		rs.close(); //closes the connection
	} catch (SQLException se) { 
		return -1; //return -1 if an error occurs
	}
	return num_countries; //number of countries
  }
  
  /**
   * Returns a string with the information of an ocean with id oid. 
   * The output is of the form oid:oname:depth. 
   * Returns an empty string if the ocean does not exist. 
   */ 
  public String getOceanInfo(int oid) {
	String ocean = "";
	try {

		ps = connection.prepareStatement("Select * from a2.Ocean where oid = ?;"); //use of a prepared statement to search for an ocean with given oid
		ps.setInt(1, oid);
		rs = ps.executeQuery();
		while (rs.next()) { //iterator
			ocean = String.valueOf(rs.getInt("oid")).trim() + ":" + rs.getString("oname").trim() + ":" + String.valueOf(rs.getInt("depth")).trim(); //formates the result for correct output
		}
		rs.close();
	} catch (SQLException se) {
		return "";
	}
	return ocean;
  }
  /**
   * Changes the HDI value of the country cid for the year year to the HDI 
   * value supplied (newHDI). Returns true if the change was successful, 
   * false otherwise.
   */
  public boolean chgHDI(int cid, int year, float newHDI) {
	try {
		ps = connection.prepareStatement("update a2.hdi set hdi_score = ? where cid = ? and year = ?;"); //updating hdi score with given cid and year
		ps.setFloat(1, newHDI);
		ps.setInt(2, cid);
		ps.setInt(3, year);
		if (ps.executeUpdate() > 0) { //checking for succesful update
			return true;
		} else {
			return false;
		}
	} catch (SQLException se) {

		return false;
	}
  }
  /**
   * Deletes the neighboring relation between two countries. 
   * Returns true if the deletion was successful, false otherwise. You can 
   * assume that the neighboring relation to be deleted exists in the database.
   * Remember that if c2 is a neighbor of c1, c1 is also a neighbour of c2.
   */
  public boolean deleteNeighbour(int c1id, int c2id) {
 	try{
		ps = connection.prepareStatement("delete from a2.neighbour where country = ? and neighbor = ?;"); //use of prepared statement
		ps.setInt(1, c1id);
		ps.setInt(2, c2id);
		if (ps.executeUpdate() > 0) { //if delete succesful
			ps = connection.prepareStatement("delete from a2.neighbour where country = ? and neighbor = ?;"); //done twice as c2 being neighbor of c1 also 
			ps.setInt(1, c2id);
			ps.setInt(2, c1id);
			if (ps.executeUpdate() > 0) {
				return true; //succesful deletion
			} else {
				return false;
			}
		} else {
			return false;
		}
	} catch (SQLException se) {	
		return false;
	}     
  }


  /**
   * Returns a string with all the languages that are spoken in the country 
   * with id cid. The list of languages is in contiguous 
   * format :l1id:l1lname:l1population#l2id:l2lname:l2population#...
   * Returns empty string if country is not found or is unsuccessful
   */
  public String listCountryLanguages(int cid) {
	  String languages = ""; //starts with empty string
	  try {
		int i = 0;
		ps = connection.prepareStatement("select l.lid as lid, l.lname as lname, 0.01*l.lpercentage*c.population as population from a2.country c inner join a2.language l on c.cid = l.cid where l.cid = ? order by population DESC;"); //use of SQL query in prepared statement
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		while (rs.next()) { //iterator
			if (i != 0) {
				languages += "#"; //for the correct formatted output
			}
			languages += rs.getString("lid").trim() + ":" + rs.getString("lname").trim() + ":" + String.valueOf(rs.getInt("population")).trim();
			i++; //formatting to remove whitespace
		}
		rs.close();
	  }catch (SQLException se) {
		  return "";
	  }
	return languages; //returning the string
  }
  /**
   * Decreases the height of the country with id cid.Returns true if the update
   * was successful, false otherwise
   */
  public boolean updateHeight(int cid, int decrH) {
	try{
		ps = connection.prepareStatement("select height from a2.country where cid = ?;"); //using cid in a prepared statement
		ps.setInt(1, cid);
		rs = ps.executeQuery();
		rs.next();
		int new_height = rs.getInt("height") - decrH; //subtracting the decreased amount from original height
		rs.close();
		ps = connection.prepareStatement("update a2.country set height = ? where cid = ?;"); //updating the new height into the table
		ps.setInt(1, new_height);
		ps.setInt(2, cid);
		if (ps.executeUpdate() == 1) {
			return true; //successful update
		} else {
			return false;
		}
	} catch (SQLException se) {
		return false;
	}
  }
  /**
   * Create a table containing all the countries which have a population 
   * over 100 million. The name of the table should be mostPopulousCountries 
   * and the attributes should 
   * be:cid INTEGER (country id),cname VARCHAR(20) (country name). Returns true 
   * if the database was successfully updated, false otherwise. Store the 
   * results in ASC order according to the country id (cid). 
   */
  public boolean updateDB() {
	try {
		sql = connection.createStatement();
		if (sql.executeUpdate("DROP TABLE IF EXISTS a2.mostPopulousCountries;") != 0) { //Drop table if already exists
			return false;
		}
		if (sql.executeUpdate("CREATE TABLE a2.mostPopulousCountries (cid 		INTEGER, cname 		VARCHAR(20)	NOT NULL);") != 0) { //creating the new table with given attributes
			return false;
		}
		sql = connection.createStatement();
		rs = sql.executeQuery("Select cid, cname from a2.country where population > 100000000 order by cid ASC;"); //using a query to search for countries with population > 1000000000
		while (rs.next()) {
			ps = connection.prepareStatement("Insert into a2.mostPopulousCountries values (?, ?) ;"); //iterating and inseritng into the new table the query reult
			ps.setInt(1, rs.getInt("cid"));
			ps.setString(2, rs.getString("cname"));
			ps.executeUpdate();
		}
		return true; //successful update
	} catch (SQLException se) { 	
		return false;
		}
	}

}
