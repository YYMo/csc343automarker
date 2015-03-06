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

  String query;
  
  //CONSTRUCTOR
  Assignment2(){
    try {
        Class.forName("org.postgresql.Driver");
    }
    catch (ClassNotFoundException e) {}
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){ 
  	try {
      connection = DriverManager.getConnection(URL, username, password);  
  	}
  	catch (SQLException se){
  		return false; 
  	}
  	return true;	
    }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){	
    try {
      connection.close();
    } 
    catch (SQLException se) {
		  return false; 
    }    
    return true; 
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
   boolean countryExists = false;
   boolean inserted = false;
    
   try{
		sql = connection.createStatement();
    query = "SELECT * FROM a2.country";
    rs = sql.executeQuery(query);

    if (rs != null){
      while (rs.next()){
        if (rs.getInt("cid") == cid)
          countryExists = true;
      }
    }

    //cid does not exist
    if (!(countryExists)){
      ps = connection.prepareStatement(
           "INSERT INTO a2.country (cid, cname, height, population) VALUES (?, ?, ?, ?)");
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population);

      ps.executeUpdate();

      if (ps.getUpdateCount() != -1)
        inserted = true;
    }

    return inserted;

	 }
	 catch (SQLException se) {
    return false;	
	 } 
  }
  
  public int getCountriesNextToOceanCount(int oid) {
	 int count = 0; 
   try {
    sql = connection.createStatement();
    query = "SELECT * FROM a2.oceanAccess;";
    rs = sql.executeQuery(query);

    if (rs != null){
      while (rs.next()){
        if (oid == rs.getInt("oid"))
          count++;
        }
      }
    }
    catch (SQLException se){
      return -1;
    }
    return count;
  }  
   
  public String getOceanInfo(int oid){
    try {
      sql = connection.createStatement();
      query = "SELECT * FROM a2.ocean;"; 
      rs = sql.executeQuery(query);

      if (rs != null) {
        while (rs.next()) {
          if (rs.getInt("oid") == oid) {
            String oceanId = rs.getString("oid");
            String oname = rs.getString("oname");
            String depth = rs.getString("depth");
            return (oceanId + ":" + oname + ":" + depth);
          }
        }
      }
    }
    catch (SQLException se) {
      return "";
    }

    return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
   boolean cidAndYearExists = false;
   boolean changed = false;

   try{
      sql = connection.createStatement();
      query = "SELECT * FROM a2.hdi";
      rs = sql.executeQuery(query);

      /* Checck if cid and year exists*/
      if (rs != null){
        while (rs.next()){
          if (rs.getInt("cid") == cid && rs.getInt("year") == year){
            cidAndYearExists = true;
            break;
          }
        }
      }

      if (cidAndYearExists){
        ps = connection.prepareStatement(
          "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? and year = ?");
        ps.setFloat(1, newHDI);
        ps.setInt(2, cid);
        ps.setInt(3, year);

        ps.executeUpdate();

        if (ps.getUpdateCount() != -1)
          changed = true;
      }
      
      return changed;
   }
   catch (SQLException se) {
    return false;
   }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    boolean relationExists = false;
    boolean deleted = false;

    try {
      sql = connection.createStatement();
      query = "SELECT * FROM a2.neighbour";
      rs = sql.executeQuery(query);

      /* Checck if relation exists*/
      if (rs != null){
        while (rs.next()){
          if (rs.getInt("country") == c1id && rs.getInt("neighbor") == c2id){
            relationExists = true;
            break;
          }
        }
      }

      if (relationExists){
       ps = connection.prepareStatement(
        "DELETE FROM a2.neighbour WHERE country = ? and neighbor = ?" +
        "or country = ? and neighbor = ?");
       ps.setInt(1, c1id);
       ps.setInt(2, c2id);
       ps.setInt(3, c2id);
       ps.setInt(4, c1id);

       ps.executeUpdate();

       if (ps.getUpdateCount() != -1)
          deleted = true;
      }
     return deleted;

    }
    catch (SQLException se) {
      return false;        
    }
  }
  
  public String listCountryLanguages(int cid){
    String languageList;
    int totalPop;
    float langPop;
    boolean first = true;
    String lid;
    String lname;
    float lpct;
    String result = "";

    try {
      sql = connection.createStatement();
      query = "SELECT * FROM a2.language join a2.country on country.cid = language.cid "
              + "ORDER BY population ASC";
      rs = sql.executeQuery(query);

      if (rs != null){
        while (rs.next()){
          if (rs.getInt("cid") == cid){
            if (!(first)){
              result += "#";
            }
            lid = rs.getString("lid");
            lname = rs.getString("lname");
            lpct = rs.getFloat("lpercentage");
            totalPop = rs.getInt("population");
            langPop = totalPop * lpct;
            result += lid + ":" + lname + ":" + langPop;
            first = false;
          }
        }
      }
    }
    catch (SQLException se){
      return "";
    }

    return result;
  }
  
  public boolean updateHeight(int cid, int decrH){
    boolean countryExists = false;
    boolean updated = false;

    try{
      sql = connection.createStatement();
      query = "SELECT * FROM a2.country";
      rs = sql.executeQuery(query);

      /* Checck if country exists*/
      if (rs != null){
        while (rs.next()){
          if (rs.getInt("cid") == cid){
            countryExists = true;
            break;
          }
        }
      }

      if (countryExists){
        ps = connection.prepareStatement(
          "UPDATE a2.country SET height = height - ? WHERE cid = ?");
        ps.setInt(1, decrH);
        ps.setInt(2, cid);

        ps.executeUpdate();

        if (ps.getUpdateCount() != -1)
          updated = true;
      }

      return updated;
   }
   catch (SQLException se) {
      return false;
   }
  }
    
  public boolean updateDB(){
    int cid;
    String cname;
    boolean updated = false;

    try{
      ps = connection.prepareStatement(
        "DROP TABLE IF EXISTS a2.mostPopulousCountries CASCADE");

      ps.executeUpdate();
	/* POPULATE TABLE!!!!!! */
	/*SELECT cid, cname INTO a2.mostPopulousCountries FROM a2.country
		WHERE population > 100000000*/
      ps = connection.prepareStatement(
        "CREATE TABLE a2.mostPopulousCountries ( " +
        "cid INTEGER, " +
        "cname VARCHAR(20) " +
        ")");

      ps.executeUpdate();

      sql = connection.createStatement();
      query = "SELECT cid, cname FROM a2.country WHERE population > 100000000";
      rs = sql.executeQuery(query);

      if (rs != null){
        while (rs.next()){
          cid = rs.getInt("cid");
          cname = rs.getString("cname");
          ps = connection.prepareStatement(
            "INSERT INTO a2.mostPopulousCountries (cid, cname)" +
            "VALUES (?, ?)");
          ps.setInt(1, cid);
          ps.setString(2, cname);

          ps.executeUpdate();

          if (ps.getUpdateCount() != -1)
            updated = true;
        }
      }

      return updated;
    }
    catch (SQLException se){
      return false;
    } 
  }

}


