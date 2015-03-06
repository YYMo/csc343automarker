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
    	Class.forName("org.postgresql.jdbc.Driver");
    } catch (Exception ex) {
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try {
      connection = DriverManager.getConnection(URL, username, password);
    } catch (SQLException ex) {
      return false;
    }
    return true;
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    boolean closed = false;  
    try {  
      connection.close();
      closed = connection.isClosed();
    } catch (SQLException ex) {
    }  
    return closed;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try {
      PreparedStatement stat = connection.prepareStatement("INSERT INTO a2.country VALUES(?, ?, ?, ?)");
      stat.setInt(1, cid);
      stat.setString(2, name);
      stat.setInt(3, height);
      stat.setInt(4, population);
      stat.executeUpdate();
      stat.close();
    } catch (SQLException ex) {
      return false;
    }
    return true;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    int numCID = 0;
    try {
      PreparedStatement checkStat = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid=?");
      checkStat.setInt(1, oid);
      ResultSet ocean = checkStat.executeQuery();
      if (ocean.next()) { 
        PreparedStatement stat = connection.prepareStatement("SELECT COUNT(*) FROM (SELECT cid FROM a2.oceanAccess WHERE oid=?) countries");
        stat.setInt(1, oid);
        ResultSet count = stat.executeQuery();
        count.next();
        numCID = count.getInt(1);
        count.close();
        stat.close();
      } else {
        numCID = -1;
      }
      ocean.close();
      checkStat.close();
    } catch (SQLException ex) {
      return -1; 
    }  
    return numCID;  
  }
   
  public String getOceanInfo(int oid){
    String oceanInfo="";
    try { 
      PreparedStatement stat = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid=?");
      stat.setInt(1, oid);
      ResultSet info = stat.executeQuery();
      if(info.next())
        oceanInfo += info.getString("oid") + ":" + info.getString("oname") + ":" + info.getString("depth");
      info.close();
      stat.close();
    } catch (SQLException ex) {
    }
    return oceanInfo;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    int updatedRows = 0;
    try {
      PreparedStatement stat = connection.prepareStatement("UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?");
      stat.setFloat(1, newHDI);
      stat.setInt(2, cid);
      stat.setInt(3, year);
      updatedRows = stat.executeUpdate();
      stat.close();
    } catch (SQLException ex) {
      return false;
    }
    return (updatedRows == 1);
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    int deletedRows = 0;
    try {
      PreparedStatement stat = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country=? AND neighbor=?");
      stat.setInt(1, c1id);
      stat.setInt(2, c2id);
      deletedRows += stat.executeUpdate();
      stat.setInt(1, c2id);
      stat.setInt(2, c1id);
      deletedRows += stat.executeUpdate();
      stat.close();
    } catch (SQLException ex) {
      return false;
    }
    return (deletedRows == 2);
  }    

  
  public String listCountryLanguages(int cid){
    String languages="";
    try {
      PreparedStatement stat = connection.prepareStatement("SELECT lid, lname, lpercentage*population AS pop FROM a2.language JOIN a2.country USING(cid) WHERE cid=? ORDER BY pop");
      stat.setInt(1, cid);
      ResultSet langset = stat.executeQuery();
      while(langset.next()) {
        if (languages != "")
          languages += "#"; 
        languages += langset.getString("lid") + ":" + langset.getString("lname") + ":" + langset.getString("pop");
      }
      langset.close();
      stat.close();      
    } catch (SQLException ex) {
    } 
    return languages;
  }
  
  public boolean updateHeight(int cid, int decrH){
    int updatedRows = 0;
    try {
      PreparedStatement stat = connection.prepareStatement("UPDATE a2.country SET height=? WHERE cid=?");
      stat.setInt(1, decrH);
      stat.setInt(2, cid);
      updatedRows = stat.executeUpdate();
      stat.close();
    } catch (SQLException ex) {
      return false;
    }
    return (updatedRows == 1);
  }
    
  public boolean updateDB(){
    try {
      PreparedStatement makestat = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries(cid INTEGER REFERENCES a2.country, cname VARCHAR(20) NOT NULL, PRIMARY KEY (cid))");
      PreparedStatement insertstat = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country WHERE population>100000000 ORDER BY cid ASC)");
      makestat.executeUpdate();
      insertstat.executeUpdate();
      makestat.close();
      insertstat.close();
    } catch (SQLException ex) {
      return false;
    }
    return true;   
  }
  
}
