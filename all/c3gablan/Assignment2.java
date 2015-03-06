/**
* csc343: Assignment 2
* submitted by: Jana Micaela Gablan
* student id: 999636069
* cdf login: c3gablan
**/

import java.sql.* ;  // for JDBC programs
import java.math.BigDecimal; 

class Assignment2 {
	/*	Connection to the database */
	Connection connection = null;
	/*	Statement to run queries */
	Statement sql;
	/*	Resultset for the query */
	ResultSet rSql;
	/* Precompiled statements */
	PreparedStatement pSql;
	/* Rows Updated output by executeUpate() */
	int rowsUpd;

	/*	Constructor */
	Assignment2 () {
		try {
			Class.forName("org.postgresql.jdbc.Driver");
		} catch (Exception e) {
			;
		}
	}
	
	/**
	* Using the input parameters, establish a connection to be used for
	* this session. Returns true if connection is sucessful
	**/
	public boolean connectDB(String url, String uname, String pass) {
		try {
			
			connection = DriverManager.getConnection(url, uname, pass);
			pSql = connection.prepareStatement("set search_path to a2");
			pSql.execute();
			pSql.close();
			return true;
		} catch (SQLException e) {
			return false;
		}
	}

	/**
	* Closes the connection. Returns true if closure was sucessful
	**/
	public boolean disconnectDB() {
		try {
			connection.close();
			if (connection.isClosed() == true){
				return true;
			}
		} catch (SQLException e) {
			return false;
		}
		return false;
	}
	
	/**
	* Inserts a row into the country table.  cid is the name of the country, name is the
	* name of the country, height is the highest elevation point and population is the 
	* population of the newly inserted country. You have to check if the country with id
	* cid exists. Returns true if the insertion was successful, false otherwise. 
	**/	
	public boolean insertCountry(int cid, String name, int height, int population) {
		try {
			if ((connection.isClosed() == true) || (connection == null)) {
				return false;
			}
			pSql = connection.prepareStatement("INSERT INTO a2.country " + 
				 "VALUES ( " + cid + ",'" 
				 	+ name + "',"+ height +","+ population + " );");
			pSql.executeUpdate();
			pSql.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	* Decreases the height of the country with id cid
	**/	
	public boolean updateHeight(int cid, int decrH) {
		try {

			if ((connection.isClosed() == true) || (connection == null)) {
				return false;
			}
			// Capture tuple from country where cid = cid
			pSql = connection.prepareStatement("CREATE VIEW TEMP AS SELECT * " +
				"FROM a2.country WHERE cid = " + cid + ";");
			pSql.execute();
			pSql = connection.prepareStatement("SELECT * FROM TEMP");
			rSql = pSql.executeQuery();
			
			if (rSql.next()){
				// Decrement height
				int h = rSql.getInt("height") - decrH;
				pSql = connection.prepareStatement("UPDATE country SET height= "
				 + h + " where cid= " + cid + ";");
				pSql.executeUpdate();

				//cleanup
				connection.commit();
				pSql = connection.prepareStatement("DROP VIEW TEMP CASCADE;");
				rSql.close();
				pSql.close();
				return true;
			}
			return false;		

		} catch (Exception e) {
			return false;
		}
	}

	/**
	* Changes the HDI value of the country cid for the year year to the
	* HDI value supplied (newHDI).
	**/		
	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			if ((connection.isClosed() == true) || (connection == null)) {
				return false;
			}
			pSql = connection.prepareStatement( "UPDATE a2.hdi set hdi_score = "
				+ newHDI + " where cid = " + cid + " and year = " + year + ";" );
			pSql.executeUpdate();
			pSql.close();
			return true;
		} catch (Exception e) {
			return false;
		}		
	}

	/**
	* Deletes the neighboring relation between two countries.
	**/
	public boolean deleteNeighbour(int c1id, int c2id) {
		try {
			if ((connection.isClosed() == true) || (connection == null)) {
				return false;
			}
			pSql = connection.prepareStatement("DELETE from a2.neighbour WHERE"
				+ " country = " + c1id + " and neighbor = " + c2id + ";");
			pSql.executeUpdate();
			pSql = connection.prepareStatement("DELETE from a2.neighbour WHERE"
				+ " country = "	+ c2id + " and neighbor = " + c1id + ";");
			pSql.executeUpdate();
			pSql.close();
			connection.commit();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	* Returns a string with all the languages that are spoken in the country with id cid.
	**/	
	public String listCountryLanguages(int cid) {
		try {
			if ((connection.isClosed() == true) || (connection == null)) {
				return "";
			}
			// Collect data to print
			pSql = connection.prepareStatement("SELECT lid, lname, lpercentage*population/100"
				+ " AS population FROM A2.language natural join A2.country " + 
				"WHERE A2.language.cid = " + cid +  
				" GROUP by lid, lname, lpercentage, A2.country.population" +
				" ORDER by population;");
			rSql = pSql.executeQuery();

			// Construct String output
			String result = "";
			if (!rSql.next() || (rSql == null)) {
				result = "";
			} else {
				do {
					if (result != ""){
						result += "#";
					}
					
					int id = rSql.getInt("lid");
					String lname = rSql.getString("lname");
					BigDecimal population = rSql.getBigDecimal("population");
					//result += Integer.toString(rSql.getInt("lid")) + ":" + rSql.getString("lname") 
					//+ ":" + Integer.toString(rSql.getInt("population")) + "#";
					result += id + ":" + lname + ":" + population;
				} while (rSql.next());
			}
			rSql.close();
			pSql.close();
			connection.commit();
			return result;
		} catch (Exception e) { 
			return "";
		}
	}

	/**
	* Returns a string with the information of an ocean with id oid. 
	**/	
	public String getOceanInfo(int oid) {
		try {
			if ((connection.isClosed() == true) || (connection == null)) {
				return "";
			}
			
			pSql = connection.prepareStatement("SELECT * FROM A2.ocean " + 
					"WHERE oid = " + oid + ";");
			rSql = pSql.executeQuery();
			
			String result = "";
			if ((!rSql.next()) || (rSql == null)) {
				result = "";
			} else {
				do {
					if (result != ""){
						result += "#";
					}
					String name = rSql.getString("oname");
					int depth = rSql.getInt("depth");

					result += oid  + ":" + name + ":" + depth;
				} while (rSql.next());
			}
			rSql.close();			
			pSql.close();
			return result;
		} catch (Exception e) { 
			return "";
		}
	}

	/**
	* Returns the number of countries in table “oceanAccess” that are
	* located next to the ocean with id oid. 
	**/	
	public int getCountriesNextToOceanCount(int oid) {
		try {
			if ((connection.isClosed() == true) || (connection == null)) {
				return -1;
			}
			pSql = connection.prepareStatement("SELECT count(*) as num " + 
				"FROM A2.oceanAccess WHERE oid = " + oid + ";");
			rSql = pSql.executeQuery();
			if (!rSql.next()) {
				return 0;
			}
			int ret = rSql.getInt("num");
			rSql.close();
			pSql.close();
			return ret;
		} catch (Exception e) {
			return -1;
		}		
	}

	/**
	* Create a table containing all the countries which have a population over 100 million
	**/	
	public boolean updateDB(){
		try {
			if ((connection.isClosed() == true) || (connection == null)) {
				return false;
			}
			
			pSql = connection.prepareStatement("DROP TABLE IF EXISTS A2.mostPopulousCountries CASCADE;");
			pSql.executeUpdate();

			// Create table
			pSql = connection.prepareStatement("CREATE TABLE" + 
				" mostPopulousCountries " +	"(cid 		INT," + 
					" cname		VARCHAR(20));");
			pSql.executeUpdate();

			// Populate table
			pSql = connection.prepareStatement("SELECT cid, cname from A2.country " 
				 + "WHERE population > 100000000 ORDER BY cid ASC;");
			rSql = pSql.executeQuery();

			// If 0 rows returned
			if (!rSql.next()) {
				return true;
			}

			int cidVal = -1;
			String nameStr = "";
			do {
				cidVal = rSql.getInt("cid");
				nameStr = rSql.getString("cname");
				pSql = connection.prepareStatement("INSERT INTO mostPopulousCountries VALUES ("
					+ cidVal + ", '"
					+ nameStr + "');");
				pSql.executeUpdate();
			} while (rSql.next());

			pSql.close();
			rSql.close();
			return true;
		} catch (Exception e) {
			return false;
		}
	}
	
}