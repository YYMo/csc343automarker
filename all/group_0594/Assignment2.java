import java.sql.*;


/* 
Created by:
Matthew Kim, 1000973789, c3kimmat
DongJoo Kim, 996869544, g3kimdon
CSC343 A2 Assignment2.java
*/
public class Assignment2 {
    
  // A connection to the database  
  Connection connection;

  //CONSTRUCTOR
  Assignment2() throws SQLException, ClassNotFoundException{
      Class.forName("org.postgresql.Driver");
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password) throws SQLException{
      PreparedStatement ps;
      try {
          // connection = DriverManager.getConnection("jdbc:postgresql:" + URL, username, password);
	  // Assuming jdbc:postresql is appended to the beginning like in example.java
	  connection = DriverManager.getConnection(URL, username, password);
	  ps = connection.prepareStatement("SET search_path TO A2");
	  ps.executeUpdate(); 
	  ps.close();
      }
      catch (SQLException ex) {
          return false;
      }
      return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB() throws SQLException {
      try {
          connection.close();
      }
      catch (SQLException ex){
          return false;
      }
      return true;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) throws SQLException {
      // Statement to run queries
      Statement sql;

      // Prepared Statement
      PreparedStatement ps;

      // Resultset for the query
      ResultSet rs;

      sql = connection.createStatement();
      String query = "SELECT cid FROM country WHERE cid = " + cid;
      rs = sql.executeQuery(query);
      if(!rs.next()) {
          ps = connection.prepareStatement("INSERT INTO country values(?, ?, ?, ?)");
          //TODO: Test if you can insert duplicate CIDs
          ps.setInt(1, cid);
          ps.setString(2, name);
          ps.setInt(3, height);
          ps.setInt(4, population);
          int update = ps.executeUpdate();
          rs.close();
          sql.close();
          ps.close();
          if (update == 1) {
              return true;
          }
          return false;
      }
      else {
          sql.close();
          rs.close();
          return false;
      }
  }
  
  public int getCountriesNextToOceanCount(int oid) throws SQLException {
    // Statement to run queries
    Statement sql;

    // Resultset for the query
    ResultSet rs;

    sql = connection.createStatement();
    String query = "SELECT COUNT(cid) FROM oceanAccess WHERE oid = " + oid;
    rs = sql.executeQuery(query);
    if (rs != null) {
        rs.next();
        int result = rs.getInt(1);
        rs.close();
        sql.close();
        return result;
    }
    else {
        rs.close();
        sql.close();
        return -1;
    }
  }
   
  public String getOceanInfo(int oid) throws SQLException{
   // Statement to run queries
   Statement sql;

   // Resultset for the query
   ResultSet rs;

   int oidresult;
   String oname;
   int depth;

   sql = connection.createStatement();
   String query = "SELECT * FROM ocean WHERE oid = " + oid;
   rs = sql.executeQuery(query);
   if (rs.next()) {
       oidresult = rs.getInt(1);
       oname = rs.getString(2);
       depth = rs.getInt(3);
       rs.close();
       sql.close();
       return oidresult + ":" + oname + ":" + depth;
   }
   else {
       rs.close();
       sql.close();
       return "";
   }
  }

  public boolean chgHDI(int cid, int year, float newHDI) throws SQLException {
   // Statement to run queries
   Statement sql;

   // Prepared Statement
   PreparedStatement ps;

   // Resultset for the query
   ResultSet rs;

   sql = connection.createStatement();
   String query = "SELECT cid, year FROM hdi WHERE cid = " + cid + "AND year = " + year;
   rs = sql.executeQuery(query);
   if(rs.next()) {
       ps = connection.prepareStatement("UPDATE hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
       ps.setFloat(1, newHDI);
       ps.setInt(2, cid);
       ps.setInt(3, year);
       int update = ps.executeUpdate();
       ps.close();
       rs.close();
       sql.close();
       if (update == 1) {
           return true;
       }
       return false;
   }
   else {
       rs.close();
       sql.close();
       return false;
   }
  }

  public boolean deleteNeighbour(int c1id, int c2id) throws SQLException {
      // Statement to run queries
      Statement sql;

      // Prepared Statement
      PreparedStatement ps;
      PreparedStatement ps2;

      // Resultset for the query
      ResultSet rs;

      sql = connection.createStatement();
      String query = "SELECT country, neighbor FROM neighbour WHERE country = " + c1id + "AND neighbor = " + c2id;
      rs = sql.executeQuery(query);
      if(rs.next()) {
          ps = connection.prepareStatement("DELETE FROM neighbour WHERE country = ? AND neighbor = ?");
          ps.setInt(1, c1id);
          ps.setInt(2, c2id);
          int update = ps.executeUpdate();
          ps2 = connection.prepareStatement("DELETE FROM neighbour WHERE country = ? AND neighbor = ?");
          ps2.setInt(1, c2id);
          ps2.setInt(2, c1id);
          int update2 = ps2.executeUpdate();
          ps.close();
          ps2.close();
          rs.close();
          sql.close();
          if (update == 1 && update2 == 1) {
              return true;
          }
              return false;
      }
      else {
          rs.close();
          sql.close();
          return false;
      }
  }
  
  public String listCountryLanguages(int cid)  throws SQLException{
    // Statement to run queries
    Statement sql;

    // Resultset for the query
    ResultSet rs;

    String answer = "";

    try {
        sql = connection.createStatement();
    }
    catch (SQLException e) {
        return "";
    }
    String query = "SELECT lid, lname, (lpercentage/100 * (SELECT population FROM country " +
            "WHERE cid = " + cid  + "))  AS population FROM language WHERE cid = " + cid + " ORDER BY population";
    try {
        rs = sql.executeQuery(query);
    }
    catch (SQLException e) {
        sql.close();
        return "";
    }
    while(rs.next()) {
        if(answer.equals("")) {
            answer = answer + rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population");
        }
        else {
            answer = answer + "#" + rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getInt("population");
        }
    }
    sql.close();
    rs.close();
	return answer;
  }
  
  public boolean updateHeight(int cid, int decrH) throws SQLException {
      // Statement to run queries
      Statement sql;

      // Prepared Statement
      PreparedStatement ps;

      // Resultset for the query
      ResultSet rs;

      sql = connection.createStatement();
      String query = "SELECT cid FROM country WHERE cid = " + cid;
      try {
      	rs = sql.executeQuery(query);
      }
      catch (Exception ex) {
	   return false;
      }
      if(rs.next()) {
          ps = connection.prepareStatement("UPDATE country SET height = ? WHERE cid = ?");
          ps.setInt(1, decrH);
          ps.setInt(2, cid);
          int update = ps.executeUpdate();
          ps.close();
          rs.close();
          sql.close();
          if (update == 1) {
              return true;
          }
          return false;
      }
      else {
          rs.close();
          sql.close();
          return false;
      }
  }
    
  public boolean updateDB() throws SQLException{
      PreparedStatement ps;
      PreparedStatement ps2;

      ps = connection.prepareStatement("CREATE TABLE mostPopulousCountries(cid INTEGER, cname VARCHAR(20))");
      ps.executeUpdate();

      ps2 = connection.prepareStatement("INSERT INTO mostPopulousCountries(SELECT cid," 
						+ "cname FROM country WHERE population > 1000000)");
      int result2 = ps2.executeUpdate();

      ps2.close();
      ps.close();
      if (result2 > 0) { return true; }
      else { return false; }
  }

  public static void main(String[] args) throws SQLException, ClassNotFoundException{
  	Assignment2 test = new Assignment2();
	boolean connect = test.connectDB("jdbc:postgresql://localhost:5432/csc343h-c3kimmat", "c3kimmat", "");
	if (connect) { System.out.println("Successfully connected."); }
	else { System.out.println("COULD NOT CONNECT!"); }
	boolean result1 = test.insertCountry(13, "North Korea", 12, 50000);
	if (result1) { System.out.println("SUCCESS: Inserted North Korea"); }
	else { System.out.println("FAILURE : Failed to insert North Korea"); }
	boolean negresult3 = test.insertCountry(1, "Canadia", 20, 1000000);
	if (negresult3) { System.out.println("FAILURE: replaced Canada with Canadia!?"); }
	else { System.out.println("SUCCESS: Could not replace Canada with Canadia"); }
	int result2 = test.getCountriesNextToOceanCount(99);
	System.out.println("Countries next to Ocean 99 Count: " + result2);
	String result3 = test.getOceanInfo(99);
	System.out.println("Ocean 99 info: " + result3);
	boolean result4 = test.chgHDI(1, 2010, (float)30.0);
	if (result4) { System.out.println("SUCCUESS: Changed country 1, year 2010 hdi to 30.0"); }
	else { System.out.println("FAILURE: Failed to change country 1, year 2010 hdi"); }
	boolean negresult2 = test.chgHDI (1, 2050, (float)90.0);
	if (negresult2) { System.out.println("FAILURE: Somehow changed HDI of country 1 year 2050"); }
	else { System.out.println("SUCCESS: Could not change HDI of country 1 year 2050");}
	boolean result5 = test.deleteNeighbour(1, 2);
	if (result5) { System.out.println("SUCCESS: Canada and the US are no longer neighbors"); }
	else { System.out.println("FAILURE: Could not separate Canada and the US, bond is too strong"); }
	boolean negresult1 = test.deleteNeighbour(6,7);
	if (negresult1) { System.out.println("FAILURE: Somehow deleted nonexistant neighbours 6 and 7"); }
	else { System.out.println("SUCCESS: Could not delete non-existant neighbours 6 and 7"); }
	String result6 = test.listCountryLanguages(1);
	System.out.println("Canada's languages: "  + result6);
	boolean result7 = test.updateHeight(11, 65);
	if (result7) { System.out.println("SUCCESS: Country 11 height lowered to 65"); }
	else { System.out.println("FAILURE: Country 11 height NOT lowered"); }
	boolean result8 = test.updateHeight(200, 40);
	if (result8) { System.out.println("FAILURE: Somehow updated country 200's height to 40"); }
	else { System.out.println("SUCCESS: Could not update country 200's height since it doesn't exist"); }
	boolean result9 = test.updateDB();
	if (result9) { System.out.println("Successfully updated DB"); }
	else { System.out.println("Failed to update DB");}
	test.disconnectDB();
  }
}
