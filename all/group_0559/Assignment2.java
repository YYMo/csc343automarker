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
	} catch (ClassNotFoundException e) {
	System.out.println("Failed to find the JDBC driver");
	}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
        try {
          connection = DriverManager.getConnection(URL, username, password);
          return true;
        } catch (SQLException e) {
            return false;
        } 
  }
  
  //Closes the connection. Returns true if closure was successful
  public boolean disconnectDB(){
      try {
          ps.close();
          rs.close();
          connection.close();
          return true;
      } catch (SQLException e) {
    	  return false;
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {

      // An error will occur if we try to replace an existing country,
      // so we do not have to check for the cid first, just catch the error.
      try {
          ps = connection.prepareStatement(
            "INSERT INTO a2.country(cid, cname, height, population)" +
            "VALUES(?, ?, ?, ?);");
		  ps.setInt(1, cid);
		  ps.setString(2, name);
		  ps.setInt(3, height);
		  ps.setInt(4, population);
		  return (ps.executeUpdate() == 1);

      } catch (SQLException e) {
          return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	  int ret;
	  try {
          ps = connection.prepareStatement(
          "SELECT COUNT(cid) AS num FROM a2.oceanAccess WHERE oid=?;");
          ps.setInt(1, oid);
          rs = ps.executeQuery();
          rs.next();
          ret = rs.getInt("num");
          return ret;
          
	  } catch (SQLException e) {
		  return -1;
	  }

  }
   
  public String getOceanInfo(int oid){
	  String ret = new String();
	  try {
          ps = connection.prepareStatement(
          "SELECT * FROM a2.ocean WHERE oid=?;");
          ps.setInt(1, oid);
          rs = ps.executeQuery();
           if (rs.next()){
              ret = rs.getString("oid") + ':' + rs.getString("oname") + ':' + rs.getString("depth");
              return ret;
           } else {
        	   return ""; }
           
	  } catch (SQLException e) {
		  return "";
	  }

  }

  public boolean chgHDI(int cid, int year, float newHDI){
	  try {
		  ps = connection.prepareStatement(
			"UPDATE a2.hdi SET hdi_score=? WHERE cid=? and year=?;");
		  ps.setFloat(1, newHDI);
		  ps.setInt(2, cid);
		  ps.setInt(3, year);
		  return (ps.executeUpdate() == 1);

	  } catch (SQLException e) {
		  return false;
	  }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
	  try {
          ps = connection.prepareStatement(
            "DELETE FROM a2.neighbour WHERE country=? and neighbor=? or"
            + " country=? and neighbor=?;");
          ps.setInt(1, c1id);
          ps.setInt(2, c2id);
          ps.setInt(3, c2id);
          ps.setInt(4, c1id);
          return (ps.executeUpdate() == 2);
	  } catch (SQLException e) {
		  return false;
	  }        
  }
  
  public String listCountryLanguages(int cid){
	  String ret = new String("");
	  try {
		  ps = connection.prepareStatement(
			"SELECT lid, lname, (population*lpercentage) as population " +
		    "FROM a2.language l, a2.country c  WHERE l.cid= c.cid"
		    + " and c.cid=? ORDER BY population;");
		  ps.setInt(1, cid);
		  rs = ps.executeQuery();
		  
		  while (rs.next()) {
			  ret = ret + "|" + rs.getInt("lid") + ":|" + rs.getString("lname")
				  + ":|" + rs.getFloat("population");
			  if (rs.isLast() == false){
			      ret = ret + "#";
			  }
		  }
		  
		  return ret;
		  
	  } catch (SQLException e) {
		  return "";
	  }
  }
  
  public boolean updateHeight(int cid, int decrH){
	  try {
		  ps = connection.prepareStatement(
			"UPDATE a2.country SET height=? WHERE cid=?;");
		  ps.setInt(1, decrH);
		  ps.setInt(2, cid);
		  return (ps.executeUpdate() == 1);

	  } catch (SQLException e) {
		  return false;
	  }
  }
    
  public boolean updateDB(){
	  try {
		  ps = connection.prepareStatement(
			"CREATE TABLE a2.mostPopulousCountries as" +
		    "(SELECT cid, cname FROM a2.country WHERE" 
			+ " population >= 100000000 ORDER BY cid ASC);");
		  ps.executeUpdate();
		  return true;
	  } catch (SQLException e) {
		  return false;
	  }  
  }
  
  
}
