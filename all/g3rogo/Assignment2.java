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
      try{
          Class.forName("org.postgresql.Driver");
      }
      catch(ClassNotFoundException e){
        //  System.err.println("Failed to find the JDBC driver");
      }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
      boolean isopen = false;
      try{
          connection = DriverManager.getConnection(URL, username, password);
	  isopen = true;
	  Statement stat = connection.createStatement();
	  stat.executeUpdate("SET search_path TO a2");
      }
      catch(SQLException se){
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  isopen = false;
      }
      if (connection != null){return isopen;}
      else{return false;}  
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      boolean isclosed = false;
      try{
          connection.close();
	  isclosed = connection.isClosed();
      }
      catch(SQLException se){
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      } 
      return isclosed;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
       boolean worked = false;
       boolean valid = false;
       try{
       PreparedStatement yesCID = connection.prepareStatement("SELECT cid FROM country WHERE cid = ?");
       yesCID.setInt(1,cid);
       ResultSet yc = yesCID.executeQuery();
       valid = !(yc.isBeforeFirst());
       yc.close();
       yesCID.close();
       }
       catch(SQLException ex){
           //System.err.println("SQL Exception." + "<Message>: " + ex.getMessage());
           worked = false;
	   valid = false;
       }
       System.out.println("cid not in country " + valid);
       if(valid){
           try{
           PreparedStatement insertStat = connection.prepareStatement("INSERT INTO country(cid, cname, height, population) values(?,?,?,?)");
           insertStat.setInt(1, cid);
           insertStat.setString(2, name);
           insertStat.setInt(3, height);
           insertStat.setInt(4, population);
           insertStat.executeUpdate();
           worked = true;
	   insertStat.close();
           }
           catch(SQLException se){
	       //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
               worked = false;}
       }
   return worked;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
      int countries;
      try{
          PreparedStatement stat = connection.prepareStatement("SELECT count(cid) FROM oceanAccess WHERE oid = ?");
	  stat.setInt(1, oid);
	  ResultSet rs = stat.executeQuery();
	  rs.next();
	  countries = rs.getInt(1);
	  rs.close();
	  stat.close();
      }
      catch(SQLException se){
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
          countries = -1;
      }
	return countries;  
  }
   
  public String getOceanInfo(int oid){
    String info;
    try{
        PreparedStatement stat = connection.prepareStatement("SELECT * FROM ocean WHERE oid = ?");
	stat.setInt(1, oid);
	ResultSet rs = stat.executeQuery();
	rs.next();
	String o = Integer.toString(rs.getInt(1));
	String n = rs.getString(2);
	String d = Integer.toString(rs.getInt(3));
	info = o + ':' + n + ':' + d;
	rs.close();
	stat.close();
    }
    catch(SQLException se){
        //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	info = "";}
   return info;
  }

  public boolean chgHDI(int cid, int year, float newHDI){
      boolean worked;
      try{
        PreparedStatement stat = connection.prepareStatement("UPDATE hdi SET hdi_score = ? WHERE cid = ? AND year = ?");
	stat.setFloat(1, newHDI);
        stat.setInt(2,cid);
	stat.setInt(3, year);
	stat.executeUpdate();
	worked = true;
	stat.close();
      }
      catch(SQLException se){
       //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	worked = false;
      }
      return worked;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
      boolean worked;
      try{
        PreparedStatement stat = connection.prepareStatement("DELETE FROM neighbour WHERE country = ? AND neighbor = ?");
        stat.setInt(1,c1id);
	stat.setInt(2,c2id);
	stat.executeUpdate();
	stat.setInt(1,c2id);
	stat.setInt(2,c1id);
	stat.executeUpdate();
	worked = true;
	stat.close();
      }
      catch(SQLException se){
        //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	worked = false;
      }
      return worked;       
  }
  
  public String listCountryLanguages(int cid){
    String info;
    try{
        PreparedStatement stat = connection.prepareStatement("SELECT lid, lname, (lpercentage * population) as lpopulation FROM language,country WHERE country.cid = ? AND country.cid = language.cid ORDER BY (lpercentage*population)");
	stat.setInt(1, cid);
	ResultSet rs = stat.executeQuery();
	rs.next();
	String o = Integer.toString(rs.getInt(1));
	String n = rs.getString(2);
	String d = Double.toString(rs.getDouble(3));
	info = o + ':' + n + ':' + d;
	while(rs.next()){
	    o = Integer.toString(rs.getInt(1));
	    n = rs.getString(2);
	    d = Double.toString(rs.getDouble(3));
	    info = info + '#' + o + ':' + n + ':' + d;
	}
	rs.close();
	stat.close();
    }
    catch(SQLException se){
        //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	info = "";}
   return info;
  }
  
  public boolean updateHeight(int cid, int decrH){
    boolean worked;
    try{
        PreparedStatement stat = connection.prepareStatement("UPDATE country SET height=(height - ?) WHERE cid = ?");
	stat.setInt(1, decrH);
        stat.setInt(2,cid);
	stat.executeUpdate();
	worked = true;
	stat.close();
    }
    catch(SQLException se){
       // System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	worked = false;
    }
    return worked;
  }
    
  public boolean updateDB(){
      boolean worked;
      try{
      PreparedStatement stat = connection.prepareStatement("CREATE TABLE mostPopulousCountries (cid INTEGER, cname VARCHAR(20))");
      stat.executeUpdate();
      stat = connection.prepareStatement("INSERT INTO mostPopulousCountries (SELECT cid, cname from country where population > 100000000 ORDER BY cid ASC)");
      stat.executeUpdate();
      worked = true;
      stat.close();
      }
      catch(SQLException se){
          //System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
	  worked = false;
      }
      return worked;    
  }

}
