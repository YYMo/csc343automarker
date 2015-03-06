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
  Assignment2(){
	  try {
		Class.forName("org.postgresql.Driver");
	    }
	    catch (ClassNotFoundException e) {
		System.out.println("Failed to find the JDBC driver");
	    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
	  System.out.println("Connecting to database...");
	  try{
      connection = DriverManager.getConnection(URL, username, password);
      }
      catch (SQLException se)
            {
                System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
                return false;
            }
      if (connection != null){return true;}
      else{return false;}
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
		  if (ps != null) {
				ps.close();
			}
		  if (rs != null) {
				rs.close();
			}
		  if (connection != null) {
				connection.close();
			}
		  System.out.println("successed exit");
		  return true;
		  }
	  catch (SQLException se){
		  System.out.println("failed exit");
		  return false;
		  }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   String s = "INSERT INTO a2.country VALUES (" + cid
			+ ", '" + name + "', " + height + ", " + population + ")";
   try{
	   sql = connection.createStatement();
	   sql.executeUpdate(s);
	   return true;
	   }
	catch (SQLException se){
		System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		  return false;
		  }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	try{
		String s = "SELECT cid FROM a2.oceanAccess WHERE oid = " + oid;
		sql = connection.createStatement();
	    rs = sql.executeQuery(s);
		int i = 0;
		while (rs.next()){i++;}
		return i;
		}
	catch (SQLException se){
		System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		return -1;}
  }
   
  public String getOceanInfo(int oid){
	String result = "";
   try{
	   String s = "SELECT * FROM a2.ocean WHERE oid =" + oid;
	   sql = connection.createStatement();
	    rs = sql.executeQuery(s);
	   while (rs.next()){
		   int ocid = rs.getInt("oid");
		   String oname = rs.getString("oname");
		   int depth = rs.getInt("depth");
		   result += Integer.toString(ocid);
		   result += ":";
		   result += oname;
		   result += ":";
		   result += Integer.toString(depth);
		   result += "#";
		   }
	   if (result.length() > 0)
	   {result = result.substring(0, result.length() - 1);}
	   return result;
	   }
	catch (SQLException se){
		System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		return "";}
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   try {
	   String s = "UPDATE a2.hdi SET hdi_score = "+ newHDI +
	    " WHERE cid = " + cid + " and year = " + year;
	   sql = connection.createStatement();
	   sql.executeUpdate(s);
	   return true;
	   }
	catch (SQLException se){
		System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		  return false;
		  }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try{
	   String s = "DELETE FROM a2.neighbour WHERE country = " +
	   c1id + " and neighbor = " + c2id;
	   sql = connection.createStatement();
	   sql.executeUpdate(s);
	   return true;
	   }
   catch (SQLException se){
	   System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		  return false;
		  }     
  }
  
  public String listCountryLanguages(int cid){
	try{
		String result = "";
		String s = "SELECT * FROM a2.language natural join a2.country WHERE cid = "
		+ cid +" ORDER BY lpercentage DESC";
		sql = connection.createStatement();
	   rs = sql.executeQuery(s);
		while (rs.next()){
			int lid = rs.getInt("lid");
		   String lname = rs.getString("lname");
		   int population = rs.getInt("population");
		   float per = rs.getFloat("lpercentage");
		   result += lid;
		   result += ":";
		   result += lname;
		   result += ":";
		   result += population * per;
		   result += "#";
			}
			System.out.println("4");
			if (result.length() > 0)
	   {result = result.substring(0, result.length() - 1);}
	   return result;
		}
	catch (SQLException se){
		System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		  return "";
		  }     
  }
  
  public boolean updateHeight(int cid, int decrH){
    try{
		String s = "UPDATE a2.country SET height = "
		+ decrH +" WHERE cid = " + cid;
		sql = connection.createStatement();
	   sql.executeUpdate(s);
		return true;
		}
    catch (SQLException se){
		System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		  return false;
		  }
  }
    
  public boolean updateDB(){
	try{
		String a = "DROP TABLE IF EXISTS mostPopulousCountries CASCADE";
		String s = "CREATE TABLE mostPopulousCountries(cid int, cname varchar(20))";
		String t = "INSERT INTO mostPopulousCountries (select cid, cname from a2.country where population > 100000000)";
		sql = connection.createStatement();
		sql.executeUpdate(a);
		sql.executeUpdate(s);
		sql.executeUpdate(t);
		return true;
		}
	catch (SQLException se){
		System.err.println("SQL Exception." +
                        "<Message>: " + se.getMessage());
		  return false;
		  }   
  } 
}
