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
  Assignment2() throws ClassNotFoundException{
	    try {
			Class.forName("org.postgresql.Driver");
	    }
	    catch (ClassNotFoundException e) {
			System.out.println("Failed to find the JDBC driver");
	    }
	    this.connection = connection;
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  try {
		this.connection = DriverManager.getConnection("jdbc:postgresql://"+URL, username, password);
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		System.out.println("Failed to connect to DataBase");
		return false;
	}
	  return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
	  try {
		this.connection.close();
		return true;
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		System.out.println("failed to close connection");
		return false;
	}
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) throws SQLException {
	String queryString = "INSERT INTO a2.country" + " VALUES (?,?,?,?);";
    PreparedStatement pStatement;
	try {
		pStatement = this.connection.prepareStatement(queryString);
	} catch (SQLException e) {
		System.out.println("Error preparing statement");
		return false;
	}
	pStatement.setInt(1, cid);
	pStatement.setString(2, name);
	pStatement.setInt(3, height);
	pStatement.setInt(4, population);
	
	try {
		pStatement.executeUpdate();
	} catch (SQLException e) {
		System.out.println(pStatement.toString());
		e.printStackTrace();
	}
    return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) throws SQLException {
		String queryString = "SELECT oid, count(country.cid) num FROM a2.oceanAccess, a2.country WHERE oceanAccess.oid = ? AND country.cid = oceanAccess.cid GROUP BY oid;";
	    PreparedStatement pStatement;
		try {
			pStatement = this.connection.prepareStatement(queryString);
		} catch (SQLException e) {
			System.out.println("Error preparing statement");
			return -1;
		}
		pStatement.setInt(1, oid);
		
		try {
			rs = pStatement.executeQuery();
			rs.next();
		    return rs.getInt("num");
		} catch (SQLException e) {
			System.out.println("Execution error");
			return -1;
		}

  }
   
  public String getOceanInfo(int oid) throws SQLException{
	 String queryString = "SELECT oid, oname, depth1 FROM a2.ocean WHERE oid = ?";
	 PreparedStatement pStatement;
	 try {
		pStatement = this.connection.prepareStatement(queryString);
	 } catch (SQLException e) {
		 System.out.println("Error preparing statement");
		 return "";
	 }
	 pStatement.setInt(1, oid);
	 try {
		 rs = pStatement.executeQuery();
		 rs.next();
		 return rs.getString("oid")+":"+rs.getString("oname")+":"+rs.getInt("depth1");
		} catch (SQLException e) {
			System.out.println("Execution error");
			return pStatement.toString();
		}
  }

  public boolean chgHDI(int cid, int year, float newHDI) throws SQLException{
   String queryString = "UPDATE a2.hdi SET hdi_score =? WHERE year1 = ? AND cid = ?";
   PreparedStatement pStatement;
	 try {
		pStatement = this.connection.prepareStatement(queryString);
	 } catch (SQLException e) {
		 System.out.println("Error preparing statement");
		 return false;
	 }
	 pStatement.setFloat(1,newHDI);
	 pStatement.setInt(2,year);
	 pStatement.setInt(3, cid);
	 try {
		 pStatement.executeUpdate();
		 return true;
		} catch (SQLException e) {
			System.out.println("Execution error"+pStatement.toString());
			return false;
		}
	 
   
  }

  public boolean deleteNeighbour(int c1id, int c2id) throws SQLException{
	  String queryString = "DELETE FROM a2.neighbour WHERE neighbour.country = ? AND neighbour.neighbor = ? OR neighbour.country = ? AND neighbour.neighbor = ?";
	   PreparedStatement pStatement;
		 try {
			pStatement = this.connection.prepareStatement(queryString);
		 } catch (SQLException e) {
			 System.out.println("Error preparing statement");
			 return false;
		 } 
		 pStatement.setInt(1, c1id);
		 pStatement.setInt(2, c2id);
		 pStatement.setInt(3, c2id);
		 pStatement.setInt(4, c1id);
		 try {
			 pStatement.executeUpdate();
			 return true;
			} catch (SQLException e) {
				System.out.println("Execution error"+pStatement.toString());
				return false;
			}
  }
  
  public String listCountryLanguages(int cid) throws SQLException{
	  String queryString = "SELECT DISTINCT lid, lname, lpercentage, population FROM a2.country, a2.language WHERE language.cid = ? AND country.cid =? ORDER BY lpercentage DESC;";
	   PreparedStatement pStatement;
	   String holder ="";
	   int pop;
		 try {
			pStatement = this.connection.prepareStatement(queryString);
		 } catch (SQLException e) {
			 System.out.println("Error preparing statement");
			 return "";
		 } 
		 pStatement.setInt(1, cid);
		 pStatement.setInt(2, cid);
		 try {
				rs = pStatement.executeQuery();
				while (rs.next()) {
					pop= rs.getInt("lpercentage")*rs.getInt("population");
					holder = holder+rs.getString("lid")+":"+rs.getString("lname")+":"+pop+"#";
				}
			    return holder;
			} catch (SQLException e) {
				System.out.println("Execution error"+pStatement.toString());
				return "";
			}
  }
  
  public boolean updateHeight(int cid, int decrH) throws SQLException{
	  String queryString = "UPDATE a2.country SET height = height -? WHERE cid = ?";
	   PreparedStatement pStatement;
		 try {
			pStatement = this.connection.prepareStatement(queryString);
		 } catch (SQLException e) {
			 System.out.println("Error preparing statement");
			 return false;
		 }
		 pStatement.setFloat(1,decrH);
		 pStatement.setInt(2, cid);
		 try {
			 pStatement.executeUpdate();
			 return true;
			} catch (SQLException e) {
				System.out.println("Execution error"+pStatement.toString());
				return false;
			}
  }
    
  public boolean updateDB(){
	  String queryString = "CREATE TABLE a2.mostPopulousCountries (cid int, cname varchar(20))";
	  PreparedStatement pStatement;
	  try {
		  pStatement = this.connection.prepareStatement(queryString);
	  } catch (SQLException e) {
		  System.out.println("Error preparing statement");
		  return false;
	  }
	  try {
		  pStatement.executeUpdate();
	  } catch (SQLException e) {
		  System.out.println("Execution error"+pStatement.toString());
		  return false;
	  }
	  String queryString2 = "INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population>10000000)";
	  PreparedStatement pStatement2;
	  try {
		  pStatement2 = this.connection.prepareStatement(queryString2);
	  } catch (SQLException e) {
		  System.out.println("Error preparing statement");
		  return false;
	  }
	  try {
		  pStatement2.executeUpdate();
		  System.out.println(pStatement2);
		  return true;
	  } catch (SQLException e) {
		  System.out.println("Execution error"+pStatement2.toString());
		  return false;
	  }

  }
  /* Testing Main
  public static void main (String[] args) throws ClassNotFoundException, SQLException{
	Assignment2 test = new Assignment2();
	test.connectDB("localhost:5432/csc343h-c2lyonss", "c2lyonss", "");
	test.insertCountry(11, "India", 40, 200000000);
	System.out.println(test.getCountriesNextToOceanCount(1));
	System.out.println(test.getOceanInfo(1));
	test.chgHDI(1, 2013, 12345678);
	test.deleteNeighbour(1, 2);
	test.updateHeight(1, 5);
	System.out.println(test.listCountryLanguages(1));
	test.updateDB();
	test.disconnectDB();
	System.out.println("hi");
  }
*/
  
}
