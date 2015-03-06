/* 	LAWRENCE TEMPLE 
	ASSIGNMENT 2 
	CSC343H - PROF DIANE HORTON
	NOVEMBER 10 2014
	THIS IS SOME RUDIMENTARY JDBC
*/


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
    //constructs the valz.
    try {
      connection = null;
      sql = null;
      ps = null;
      rs = null;
      Class.forName("org.postgresql.Driver"); //load the driver
    } catch (ClassNotFoundException ex) {
      //whu oh
      return;
    }
  } 
  			 
  
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    
    //trys to connect
    try{
      connection = DriverManager.getConnection(URL, username, password);
    } catch (SQLException ex) {
      return false;
    }
    
    //makes sure there is a real connection
    if (connection == null) return false;
    else {
      return true;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
  
    //trys to disconnect
    try {
      connection.close();
      if (sql != null) sql.close();
      if (ps != null) ps.close();
      if (rs != null) rs.close();
    } catch (SQLException ex) {
      return false;    
    }
    return true;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
   
      //checks for a country with the same CID
      ps = connection.prepareStatement("SELECT cid FROM a2.country WHERE cid = ?");
      ps.setInt(1, cid);
      ResultSet cidExists = ps.executeQuery();
      boolean alreadyCountry = cidExists.next();
      cidExists.close();
      
      //if there is already a country with this cid, returns false
      if (alreadyCountry == true) return false;
     
	
      //inserts the new data into the table
      ps = connection.prepareStatement("INSERT INTO a2.country(cid, cname, height, population) VALUES(?, ?, ?, ?)");
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population); 
      int updateResult = ps.executeUpdate() ;
      
      cidExists.close();
      //makes sure the insert went okay
      if (updateResult == 1) return true;
      else return false;
    } catch (SQLException ex) {
      return false;
    }
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try {
    
      ps = connection.prepareStatement("SELECT count(cid) FROM a2.oceanAccess where oid = ?");
      ps.setInt(1, oid);
      ResultSet oceans = ps.executeQuery();
    
      
      //Makes sure there is data, returns it
      if (oceans.next() == true) {
	int numOceans = oceans.getInt(1);
	oceans.close();
	return numOceans;
      } else {
        oceans.close();
        return -1;
      }
    } catch (SQLException ex) {
      return -1;
    }
  }
  
  public String getOceanInfo(int oid){
    try {

      ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
      ps.setInt(1,oid);
      ResultSet oceanData = ps.executeQuery();
      
      //Makes sure there is data, returns it
      if (oceanData.next() == true) {
	String retData = oceanData.getInt(1) + ":" + oceanData.getString(2) + ":" + oceanData.getInt(3);
	oceanData.close();
	return retData;
      } else {
	oceanData.close();
	return "";
      }
    } catch (SQLException ex) {
      return "";
    } 
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try {
      //Checks that there is already an HDI, if there isnt returns false
      ps = connection.prepareStatement("SELECT * FROM a2.hdi");
      ResultSet hdiExist = ps.executeQuery();
      boolean alreadyHDI = hdiExist.next();
      hdiExist.close();
      
      if (alreadyHDI == false) return false;
            
      //Update the HDI
      ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
      ps.setInt(2, cid);
      ps.setInt(3, year);
      ps.setFloat(1, newHDI);
      int changeSuccess = ps.executeUpdate();
      if (changeSuccess == 1) return true;
      else return false;
    } catch (SQLException ex) {
      return false;
    }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try {
      //CHECKS BOTH RELATIONS EXIST
      ps = connection.prepareStatement("SELECT * FROM a2.neighbour WHERE country = ? AND neighbor = ?");
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      ResultSet c1c2Exist = ps.executeQuery();
      
      boolean coolNeighbor = c1c2Exist.next();
      c1c2Exist.close();
      if (coolNeighbor == false) return false;
      
      ps = connection.prepareStatement("SELECT * FROM a2.neighbour WHERE country = ? AND neighbor = ?");
      ps.setInt(2, c1id);
      ps.setInt(1, c2id);
      ResultSet c2c1Exist = ps.executeQuery();
      coolNeighbor = c2c1Exist.next();
      if (coolNeighbor == false) return false;
      
      //exec both updates
      ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      int c1c2 = ps.executeUpdate();
      if (c1c2 != 1) return false;
      ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? AND neighbor = ?");
      ps.setInt(2, c1id);
      ps.setInt(1, c2id);
      int c2c1 = ps.executeUpdate();
      if (c2c1 != 1) return false;
      
      return true;
      
    } catch (SQLException ex) {
      return false;        
    }
  }
  
  public String listCountryLanguages(int cid){
    try {
    
      // finds the language data for given cid
      ps = connection.prepareStatement("SELECT l.lid as lid, l.lname as lname, (l.lpercentage * c.population) as lpopulation FROM a2.language l JOIN a2.country c ON (l.cid = c.cid) WHERE c.cid = ? ORDER BY lpopulation");
      ps.setInt(1, cid);
      ResultSet langz = ps.executeQuery();
      
      //parses it and returns it in retData
      String retData;
      retData = "";
      while (langz.next()) {
	retData += langz.getInt(1) + ":" + langz.getString(2) + ":" + langz.getInt(3);
	if (langz.next()) retData += "#";
      }
      langz.close();
      return retData;
      
    } catch (SQLException ex) {
     return "";
    }
  }
  
  public boolean updateHeight(int cid, int decrH){
    try {
      //Finds old height
      ps = connection.prepareStatement("SELECT height FROM a2.country WHERE cid = ?");
      ps.setInt(1, cid);
      ResultSet oldHeight = ps.executeQuery();
      
      //makes sure there really is an old height
      if (oldHeight.next() == false) {
	oldHeight.close();
	return false;
      }
      
      //calcs the new height
      int oH = oldHeight.getInt(1);
      int nH = oH-decrH;
      oldHeight.close();
      
      //preps the new height
      ps = connection.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?");
      ps.setInt(1, nH);
      ps.setInt(2, cid);
      int newHeightSet = ps.executeUpdate();
      
      //makes sure the update pushed the new height
      if (newHeightSet != 1) return false;
      else return true;
      
    } catch (SQLException ex) {
      return false;
    }
  }
    
  public boolean updateDB(){
    try {
      
      //creates the table and make sure nothing went funny
      ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries (cid INT, cname varchar(20))");
      int newTableHappen = ps.executeUpdate();
      if (newTableHappen < 0) return false;

      //populates the table and makes sure nothing was HILARIOUS
      ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC)");
      int newTableInsert = ps.executeUpdate();
      if (newTableInsert < 0) return false;
      return true;
      
    } catch (SQLException ex) {
      return false;    
    }
  }
  
}
