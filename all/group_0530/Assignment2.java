import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
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
	Assignment2() {
		try {
			Class.forName("org.postgresql.Driver");
		} catch (ClassNotFoundException e){
			System.out.println("Please include your PostgreSQL JDBC Driver "
					+ "in your libarary path.");
			e.printStackTrace();
		}
	}
		  
	//Using the input parameters, 
	//establish a connection to be used for this session. 
	//Returns true if connection is successful
	public boolean connectDB(String URL, String username, String password){
		try {
			connection = DriverManager.getConnection(URL, username, password);
		} catch (SQLException e) {
			System.out.println("ERROR: Connection failed! Check output console!");
			e.printStackTrace();
			return false;
		}
		
		if (connection != null) {
			return true;
		} else {
			return false;
		}
	}
		  
	//Closes the connection. Returns true if closure was successful
	public boolean disconnectDB(){
		try {
			connection.close();
		} catch (SQLException e) {
			System.out.println("ERROR: Cannot close connection!");
			e.printStackTrace();
			return false;
		}
		return true;    
	}
		   
	//Inserts a row into the country table. cid is the name of the country, name is the 
	//name of the country, height is the highest elevation point and population is the 
	//population of the newly inserted country. You have to check if the country with id 
	//cid exists. Returns true if the insertion was successful, false otherwise. 
	public boolean insertCountry (int cid, String name, int height, int population) {
		try {
			sql = connection.createStatement();
			String checkExist = "SELECT * FROM country WHERE cid = " + cid;
			ResultSet rs = sql.executeQuery(checkExist);

			if (!rs.next()) {
				String sqlText = "INSERT INTO country VALUES(" + cid 
						+ ",\'" + name + "\'," + height + "," + population +");";
				sql.executeUpdate(sqlText);
				rs.close();
				sql.close();
				return true;
			} else {
				rs.close();
				sql.close();
				return false;
			}
		} catch (SQLException e) {
			System.out.println("ERROR: Update of insertCountry failed!");
			e.printStackTrace();
			return false;
		}
	
	}
	
	//Returns the number of countries in table “oceanAccess” 
	//that are located next to the ocean with id oid. 
	//Returns -1 if an error occurs
	public int getCountriesNextToOceanCount(int oid){
		try {
			sql = connection.createStatement();
			String sqlText = "SELECT COUNT(oa.cid) FROM ocean o, oceanAccess oa "
					+ "WHERE o.oid = " + oid + "GROUP BY oa.cid"; 
			rs = sql.executeQuery(sqlText);
			int result = -1;
			
			if (!rs.next()) {
				rs.close();
				sql.close();
				return -1;
			} else {
				result = rs.getInt(1);
				rs.close();
				sql.close();
				return result;
			}
		} catch (SQLException e) {
			System.out.println("ERROR: Query of getCountriesNextToOceanCount failed!");
			e.printStackTrace();
			return -1;
		} 
	}
	
	//Returns a string with the information of an ocean with id oid. The output is 
	//“oid:oname:depth”. Returns an empty string “” if the ocean does not exist.
	public String getOceanInfo(int oid){
		String result =  "";
		try {
			sql = connection.createStatement();
			String sqlText = "SELECT oid, oname, depth FROM ocean WHERE oid = " + oid;
			rs = sql.executeQuery(sqlText);
			if (!rs.next()) {
				rs.close();
				sql.close();
				return "";
			} else {
				result = rs.getString(1) + ":" + rs.getString(2) + ":" 
							+ rs.getString(3);
				rs.close();
				sql.close();
				return result;
			}
		} catch (SQLException e){
			System.out.println("ERROR: Query of getOceanInfo failed!");
			e.printStackTrace();
			return "";
		}
	}
	
	//Changes the HDI value of the country cid for the year year to the HDI value 
	//supplied (newHDI). Returns true if the change was successful, false otherwise.
	public boolean chgHDI(int cid, int year, float newHDI) {
		try {
			sql = connection.createStatement();
			String checkExist = "SELECT * FROM hdi WHERE cid = " + cid 
					+ "AND year = " + year;
			rs = sql.executeQuery(checkExist);

			if (rs.next()) {
				String sqlText = "UPDATE hdi SET hdi_score = " + newHDI 
						+ "WHERE cid = " + cid + "AND year = " + year;
				sql.executeUpdate(sqlText);
				rs.close();
				sql.close();
				return true;
			} else {
				rs.close();
				sql.close();
				return false;
			}
		} catch (SQLException e) {
			System.out.println("ERROR: Update of chgHDI failed!");
			e.printStackTrace();
			return false;
		}
	}
	
	public boolean deleteNeighbour(int c1id, int c2id) {
	        try {
		            this.sql = this.connection.createStatement();
		            this.sql.executeUpdate("DELETE FROM neighbour WHERE country = "
		                    + c1id + " AND neighbor = " + c2id);
		            this.sql.executeUpdate("DELETE FROM neighbour WHERE country = "
		                    + c2id + " AND neighbor = " + c1id);
		            this.sql.close();
		        } catch (SQLException e) {
		            System.err.println(e);
		            return false;
		        }
		        return true;
	}
	
	//Returns a string with all the languages that are spoken in the country with id cid. 
	//The list of languages should follow the contiguous format described above, and 
	//contain the following attributes in the order shown: (NOTE: before creating the 
	//string order your results by population).
	//Returns an empty string “” if the country does not exist
		  public String listCountryLanguages(int cid){
			  StringBuilder sb = new StringBuilder();
			  int percentage;
			  String lname;
			  int lid;
			  int lpopulation;
	
			  try{
				  sql = connection.createStatement();
			      String sqlText;
			      System.out.println("successful");
			      //Get the needed lid, cname, population 

			      sqlText = "SELECT l.lid, l.lname, c.population*l.lpercentage "
			    		  +"AS population "
		          		+ "FROM country c, language l "
		          		+ "WHERE c.cid = l.cid AND l.cid = " + cid
		          		+ " ORDER BY population DESC";
			      rs = sql.executeQuery(sqlText);

		          //format the result
		            while (rs.next()){
		                	  
		            	lpopulation = rs.getInt("population");
			            lname = rs.getString("lname");
			            lid = rs.getInt("lid");
			            sb.append(lid+":");
			            sb.append(lname+":");
			            sb.append(lpopulation+"#");

		          }
		            if (sb.length() > 0) { // remove last #
		            	sb.deleteCharAt(sb.length()-1);
		            }
		            this.rs.close();
		            this.sql.close();
		          
			  } catch (SQLException e) {
				  	System.out.println("ERROR: Update of listCountryLanguages failed!");
					e.printStackTrace();
			        return "";
			  }
			  return sb.toString();
		  }
		  
		  //Decreases the height of the country with id cid. (A decrease might happen due to 
		  //natural erosion.) Returns true if the update was successful, false otherwise.
		  public boolean updateHeight(int cid, int decrH){
			    int updateHeight;
				String sqlText;
				try {
					sql = connection.createStatement();
					sqlText = "SELECT height FROM country WHERE cid = " + cid;
					rs = sql.executeQuery(sqlText);
					if(rs.next()){
						updateHeight = rs.getInt(1)- decrH;
						sqlText = "UPDATE country SET height = "+ updateHeight 
								+ " WHERE cid = " + cid;
						sql.executeUpdate(sqlText);
						rs.close();
						sql.close();
					}
					else {
		                rs.close();
	                    sql.close();
						return false;
					}
				} catch (SQLException e) {
					System.out.println("ERROR: Update of updateHeight failed!");
					e.printStackTrace();
					return false;
				}
				
				return true;
			  }
		  
		  //Create a table containing all the countries which have a population over 100 
		  //million. 
		    public boolean updateDB() {
		        try {
		            String tableName = "mostPopulousCountries";

		            this.sql.executeUpdate("CREATE TABLE " + tableName
		                    + " AS SELECT cid, cname FROM country "
		                    + "WHERE population > 100000000 ORDER BY cid ASC");
		            this.sql.close();
		        } catch (Exception e) {
		            System.out.println(e);
		            return false;
		        }
		        return true;
		    }
		  
		    //main was used for testing purpose and now is commented out
	/*	    
			public static void main(String[] argv) {
			
				String LOCAL_HOST_URL = "jdbc:postgresql://localhost:5432/";
				String URL = null, username = null, password=null;
				
				//read input of url for database connection        
		        System.out.println("Enter URL for database connection:");
		        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		        try {
					URL = br.readLine();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
		        
		        System.out.println("Enter username:");
		        br = new BufferedReader(new InputStreamReader(System.in));
		        try {
					username = br.readLine();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
		        
		        System.out.println("Enter password:");
		        br = new BufferedReader(new InputStreamReader(System.in));
		        try {
					password = br.readLine();
				} catch (IOException e1) {
					e1.printStackTrace();
				}

				Assignment2 a2 = new Assignment2();
		     
		        try {
					a2.connectDB(LOCAL_HOST_URL+URL, username, password);
				} catch (Exception e) {
					System.out.println("Please include your PostgreSQL JDBC Driver "
							+ "in your libarary path.");
					e.printStackTrace();
				}
		        
		        System.out.println(a2.listCountryLanguages(1));
		    
			}

	*/		
	  
}
