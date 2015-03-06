
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
  
  //A string for random use
	String sqlText;
  
  //CONSTRUCTOR
  Assignment2(){
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  
	  try {
		Class.forName("org.postgresql.Driver");
	} catch (ClassNotFoundException e) {
		return false;
	}
	  try {
		  connection = DriverManager.getConnection
		  (URL, username, password);
		  sql = connection.createStatement();
	} catch (SQLException e) {
		return false;
	}
	   return true;
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
	  try {
		  sql.close();
		connection.close();
	} catch (SQLException e) {
		return false;
	}   
	return true;
  }
  /*Inserts a row into the country table with the following attributes: 
  * cid: the id of the country
  * name: the name of the country
  * height: the highest elevation point of the country
  * population: the population of the country
  * The function returns true if the insertion was successful, false otherwise. 
	*/
  public boolean insertCountry (int cid, String name, int height, int population) {
	try {
		
		rs = sql.executeQuery ("select cid, cname from country where cid=" + cid );
		
		//Iterates over all the entries in the database to see if the cid already exists.
		while( rs.next() ){
			 //If a duplicate cid is found then insert failed.
			 if (rs.getInt(1) == cid){
				 return false;
			 }
		}
	}catch (SQLException e1) {}
	  
	sqlText = "INSERT INTO country VALUES (" +cid +", '" + name +"', "+height+", "+population+")";
	
	try {
		
		//Performs query to insert country into the database.
		sql.executeUpdate(sqlText);
	
	} catch (SQLException e) {
		return false;
	}
	return true;
  }
  
 /* Returns the number of countries in table oceanAccess that are 
  * located next to the ocean with id oid. 
  * Returns -1 if an error occurs.
  */
  public int getCountriesNextToOceanCount(int oid) {
	
	  int answer = 0;
	  try {
			rs = sql.executeQuery ("select * from oceanAccess where oid=" + oid );
			
			//Iterates over all the countries in the oceanAccess table 
			//with access to ocean oid and increments the counter.
			while( rs.next() ){
				answer++;
			}
			return answer;
			
		} catch (SQLException e1) {
			return -1;
		}  
  }
   

  /*
   * Returns a string with the information of an ocean with id oid. 
   * Returns an empty string if the ocean does not exist.
   */
  public String getOceanInfo(int oid){
	  
	  String answer = "";
	  
	  try {
		  
			rs = sql.executeQuery ("select * from ocean where oid=" + oid );
			while( rs.next() ){
				 answer = (rs.getInt(1) + ":" + rs.getString(2) + ":" + rs.getInt(3));
				 return answer;
			}
			
			return "";
		
		} catch (SQLException e1) {
			return "";
		}
  }


  /*Changes the HDI value of the country cid for the year year to the 
   * HDI value supplied (newHDI). Returns true if the change was 
   * successful, false otherwise.
   */ 
   public boolean chgHDI(int cid, int year, float newHDI){
	  
	  boolean exists=false;
	  
	  try {
			rs = sql.executeQuery ("select * from hdi  where cid=" + cid + "and year=" + year);
		
			//sets exists to true if there is a country with id cid.
			while( rs.next() ){
				 exists=true;
			}
			
			//There is no country with id cid.
			if (!exists){
				return false;
			}
		
		} catch (SQLException e1) {
			return false;
		}  
	  	  
	  try {
		
		  sqlText = "update hdi set hdi_score =" + newHDI +
			" where cid=" + cid + "and year=" + year;

			//Executes update of hci values for country with id cid.
			sql.executeUpdate(sqlText);
			return true;

		} catch (SQLException e1) {
			return false;
		}

  }

/*
 * Deletes the neighboring relation between two countries. Returns true
 * if the deletion was successful, false otherwise. 
 */ 
  public boolean deleteNeighbour(int c1id, int c2id){
	  
	  try {
		  sqlText = "DELETE FROM neighbour WHERE country =" + c1id +
			" and neighbor=" + c2id;
			
			//Executes query to remove neighbour 2 from neighbour 1's
			// neighbours.
			sql.executeUpdate(sqlText);
			
			 sqlText = "DELETE FROM neighbour WHERE country =" + c2id +
				" and neighbor=" + c1id;
			
			//Executes query to remove neighbour 1 from neighbour 2's
			// neighbours.
			sql.executeUpdate(sqlText);


			return true;
		} catch (SQLException e1) {
			return false;
		}
    
  }
  /*Returns a string with all the languages that are spoken in the 
   * country with id cid. 
   */ 
  public String listCountryLanguages(int cid){
	  
	  int pop = -1;
	  String answer = "";
	  int count = 1;
	  
	  try {
			rs = sql.executeQuery ("select population from country where cid=" + cid );
			
			//pop represents the population of the country we find 
			while( rs.next()){
				pop = rs.getInt(1);
			}
			
			 if (pop == -1){
				 //This means the country didn't exist
				 return "";
			 }
		} catch (SQLException e1) {
			return "";
		}
	  
		  try {
				rs = sql.executeQuery ("select * from language where cid=" + cid );
		
				while( rs.next()){

					answer = answer + "l" + count+ "id:" + rs.getInt(2) + 
					"l" + count+ "name:" + rs.getString(3) +
					"l" + count+ "population#" + rs.getFloat(4) * pop;
					count++;
					}

			} catch (SQLException e1) {
				return "";
			}
		
			return answer;
  }
  

  /*Decreases the height of the country with id cid. Returns true if
   *  the update was successful, false otherwise.
   */ 
  public boolean updateHeight(int cid, int decrH){
	  //Check if the tuple is inside
	  boolean exists=false;
	  try {
			rs = sql.executeQuery ("select * from country  where cid=" + cid);
			while( rs.next() ){
				 exists=true;
				}
			if (!exists){
				//This means the country doesn't exist
			return false;
			}
		} catch (SQLException e1) {
			return false;
		}  
	  
	  
	  try {
		  sqlText = "update country set height = height-" + decrH +
			" where cid=" + cid;
			sql.executeUpdate(sqlText);

			return true;
		} catch (SQLException e1) {
			return false;
		}
	 
  }
/*Creates a table containing all the countries which have a 
 * population over 100 million. Returns true if the database was 
 * successfully updated, false otherwise    
 */ 
  public boolean updateDB(){
	  String tempName;
	  int tempID;

	
	try {
		sqlText = "DROP TABLE mostPopulousCountries";
		sql.executeUpdate(sqlText);
	} catch (SQLException e) {
	}
	

	try {
		Statement tempS = connection.createStatement();
		sqlText = "CREATE TABLE mostPopulousCountries(cid int, cname VARCHAR(20))";
		sql.executeUpdate(sqlText);
		rs = sql.executeQuery ("select * from country  where population>100000000 ORDER BY" +
				" cid ASC ");
		while(rs.next() ){
			tempID = rs.getInt(1);
			tempName = rs.getString(2);
			 sqlText = "INSERT INTO mostPopulousCountries VALUES (" + tempID+", '" + tempName +"')";
			 tempS.executeUpdate(sqlText);
			}
		tempS.close();
		
		return true; 
	} catch (SQLException e) {
		return false; 
	}

  }

  

}