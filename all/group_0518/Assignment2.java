
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
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      try{
        connection = DriverManager.getConnection(URL, username, password);
        return true;
      }
      catch(SQLException se){
        return false;
      }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try{
        connection.close();
        return true;    
      }
      catch(SQLException se){
        return false;
      }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try{
       ps = connection.prepareStatement("SELECT * FROM a2.country WHERE cid = ?");
       ps.setInt(1, cid);
       rs = ps.executeQuery();
       if (rs.next()) // cid already exists
           return false;
       ps = connection.prepareStatement("INSERT INTO a2.country(cid, cname, height, population)"
                                        + " VALUES (?, ?, ?, ?)");
       ps.setInt(1, cid);
       ps.setString(2, name);
       ps.setInt(3, height);
       ps.setInt(4, population);
       ps.executeUpdate();
       return true;
    }
    catch(SQLException se){
       return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      try{
        ps = connection.prepareStatement("SELECT count(*) FROM a2.oceanAccess WHERE oid = ?");
        ps.setInt(1, oid);
        rs = ps.executeQuery();
		if (rs.next())
      	  return rs.getInt("count");
		return -1;
      }
      catch(SQLException se){
        return -1;
      }
  }
   
  public String getOceanInfo(int oid){
    try{
        ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
        ps.setInt(1, oid);
        rs = ps.executeQuery();
        String answer = "";
        if(rs.next()) // If it exists, return the string
            return rs.getInt("oid") +":"+ rs.getString("oname") +":"+ rs.getInt("depth");
        return "";
      }
    catch(SQLException se){
        return "";
    }
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   try{
        ps = connection.prepareStatement("SELECT * FROM a2.hdi WHERE cid = ? AND year = ?");
        ps.setInt(1, cid);
        ps.setInt(2, year);
        rs = ps.executeQuery();
        if (rs.next()){
            ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            ps.executeUpdate();
            return true;
        }
        return false;
      }
      catch(SQLException se){
        return false;
      }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
   try{
        ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
        ps.setInt(1, c1id);
        ps.setInt(2, c2id);
        ps.executeUpdate();
        ps.setInt(1, c2id);
        ps.setInt(2, c1id);
        ps.executeUpdate();
        return true;
      }
      catch(SQLException se){
        return false;
      }      
  }
  
  public String listCountryLanguages(int cid){
	try{
        ps = connection.prepareStatement("SELECT population FROM a2.country WHERE cid = ?");
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        if (!rs.next())
            return "";
        int population = rs.getInt("population");
        ps = connection.prepareStatement("SELECT lid, lname, lpercentage FROM a2.language WHERE cid = ? ORDER BY lpercentage");
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        String result = "";
        while(rs.next()){
            result += rs.getInt("lid") + ":" + rs.getString("lname") + ":"+
                    (rs.getFloat("lpercentage")*population) +"#";
        }
        return result.substring(0, result.length()-1);
      }
      catch(SQLException se){
        return "";
      }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try{
        ps = connection.prepareStatement("SELECT * FROM a2.country WHERE cid = ?");
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        if (!rs.next()) // cid doesn't exists
            return false;
        ps = connection.prepareStatement("UPDATE a2.country SET height = (height - ?) WHERE cid = ?");
        ps.setInt(1, decrH);
        ps.setInt(2, cid);
        ps.executeUpdate();
        return true;
      }
      catch(SQLException se){
        return false;
      }
  }
    
  public boolean updateDB(){
	try{
        ps = connection.prepareStatement("DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE");
        ps.executeUpdate();
        ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries(cid INTEGER, cname VARCHAR(20))");
        ps.executeUpdate();
        ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries "
                                        + "(SELECT cid, cname FROM a2.country WHERE population > 1e8 ORDER BY cid)");
        ps.executeUpdate();
        return true;
      }
      catch(SQLException se){
        return false;
      }  
  }
  
// Commented out code for testing Assignment2
/*
 public static void main(String args[]){
	Assignment2 a2 = new Assignment2();
	boolean connected = a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3sophia", "g3sophia", "");
	if (connected){
		if (a2.insertCountry (999599, "Sophialand", 163, 1))
			System.out.println("Sophialand successfully inserted!");
		else
			System.out.println("Failed to insert.");

		// Testing getCountriesNextToOceanCount
		System.out.println("Countries next to oid 1: "+ a2.getCountriesNextToOceanCount(1));
		System.out.println("Countries next to oid 50: "+ a2.getCountriesNextToOceanCount(50));

		// Testing getOceanInfo
		System.out.println("Ocean Info of oid 1: "+ a2.getOceanInfo(1));
		System.out.println("Ocean Info of oid 50: "+ a2.getOceanInfo(50));

		// Testing chgHDI
		if (a2.chgHDI(1, 2013, 0.398f))
			System.out.println("Successfully changed HDI for 2013!");
		else
			System.out.println("Could not change HDI for 2013.");
		if (a2.chgHDI(1, 2050, 0.398f))
			System.out.println("Successfully changed chgHDI!");
		else
			System.out.println("Could not change HDI for 2050.");
		
		// Testing deleteNeighbour
		// Make sure to call insert into neighbour values (999159, 999599, 69);
		//					 insert into neighbour values (999599, 999159, 69);
		if (a2.deleteNeighbour(999599, 999159))
			System.out.println("Neighbour deletion successful!");
		else
			System.out.println("Neighbour deletion failed.");

		// Testing listCountryLanguages
		System.out.println("Languages of country with oid 1: "+ a2.listCountryLanguages(1));
		System.out.println("Languages of country with oid 6969: "+ a2.listCountryLanguages(6969));

		// Testing updateHeight
		if (a2.updateHeight(999159, 1))
			System.out.println("Successfully decreased height of 999159!");
		else
			System.out.println("Could not decrease height of 999159.");

		if (a2.updateHeight(6969, 1))
			System.out.println("Successfully decreased height of 6969!");
		else
			System.out.println("Could not decrease height of 6969.");

		// Testing updateDB()
		if (a2.updateDB())
			System.out.println("mostPopulousCountries created!");
		else
			System.out.println("Failed to create mostPopulousCountries.");

		if (a2.disconnectDB())
			System.out.println("Disconnected successfully.");

	}
}
*/ 
}


