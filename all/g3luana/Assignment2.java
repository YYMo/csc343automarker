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
    try
      {
        connection = DriverManager.getConnection(URL, username, password);      
      }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }

    if (connection != null){
      System.out.println("Connection made");
      return true;
    }
    else {
      System.out.println("Connection FAILED");
      return false;    
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try
      {
      connection.close();  
      return true;
      }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
    return false;
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {
    try
    {
      String sqlText;
      sqlText = "insert into country values (?, ?, ?, ?)";
 
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      ps.setString(2, name);
      ps.setInt(3, height);
      ps.setInt(4, population);
      ps.executeUpdate();
      ps.close();
      return true;
    }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
    
    return false;
  }
  
  public int getCountriesNextToOceanCount(int oid) {
    try
    {
      String sqlText;
      sqlText = "select count(cid) as cidcount from oceanAccess where oid=?";

      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      while (rs.next()) {
        int c = rs.getInt("cidcount");
        return c;
      }
      ps.close();
    }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }  
    return -1;  
  }
   
  public String getOceanInfo(int oid){
    try
    {
      String sqlText;
      sqlText = "select oid, oname, depth from ocean where oid=?";
      
      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, oid);
      rs = ps.executeQuery();
      while (rs.next()) {
        return (rs.getInt("oid") + ":" + rs.getString("oname") + ":" + rs.getInt("depth"));
      }
      ps.close();
    }
    catch (SQLException se)
      {
       System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
   return "";
  }

  public boolean chgHDI(int cid, int year, float newHDI){
    try
    {
      String sqlText;
      sqlText = "update hdi set hdi_score = ? where cid = ? and year = ?";

      ps = connection.prepareStatement(sqlText);
      ps.setInt(2, cid);
      ps.setInt(3, year);
      ps.setFloat(1, newHDI);
      ps.executeUpdate();
      ps.close();
      return true;
    }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
    return false;
  }

  public boolean deleteNeighbour(int c1id, int c2id){
    try
    {
      String sqlText;
      sqlText = "delete from neighbour where (country = ? and neighbor = ?) or (country = ? and neighbor = ?)";

      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, c1id);
      ps.setInt(2, c2id);
      ps.setInt(3, c2id);
      ps.setInt(4, c1id);
      ps.executeUpdate();
      ps.close();
      return true;
    }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
    return false;
  }
  
  public String listCountryLanguages(int cid){
    try
    {
      String sqlText;
      sqlText = "select language.lid as lid, language.lname as lname, country.population*language.lpercentage as population from country join language on country.cid=language.cid where country.cid=?";
      String returnString = "";

      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, cid);
      rs = ps.executeQuery();

      int count = 1;
      while (rs.next()) {
        returnString = returnString + ("|"+count+"id:" + rs.getInt("lid") + 
"|"+count+"name:" + rs.getString("lname") + 
"|"+count+"population:" + rs.getInt("population") + "#");
        count++;
      }
      ps.close();
      returnString = returnString.substring(0, (returnString.length() - 1));
      return returnString;
    }
    catch (SQLException se)
      {
       System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
    return "";
  }
  
  public boolean updateHeight(int cid, int decrH){
    try
    {
      String sqlText;
      sqlText = "update country set height = (height - ?) where cid = ? order by cid ASC";

      ps = connection.prepareStatement(sqlText);
      ps.setInt(1, decrH);
      ps.setInt(2, cid);
      ps.executeUpdate();
      ps.close();
      return true;
    }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
    return false;
  }
    
  public boolean updateDB(){
    try
    {
      sql = connection.createStatement();
      String sqlText;
      sqlText = "CREATE TABLE mostPopulousCountries( cid INTEGER, cname VARCHAR(20))";
      sql.executeUpdate(sqlText);
      String sqlText2;
      sqlText2 = "INSERT INTO mostPopulousCountries(select cid, cname from country where population > 100000000)";
      sql.executeUpdate(sqlText2);
      return true;
    }
    catch (SQLException se)
      {
        System.err.println("SQL Exception." + "<Message>: " + se.getMessage());
      }
    return false;    
  }

  //**************COMMENTING OUT MAIN*************************

/*  public static void main(String[] args){
    Assignment2 a2 = new Assignment2();
    if (a2.connectDB("jdbc:postgresql://localhost:5432/csc343h-g3luana", "g3luana", "")){
      System.out.println("you're connected to the database");
    }
 
//    if (a2.insertCountry(16, "Paraguay", 8, 823)){
//      System.out.print("I think it worked :)");
//    }

    if (a2.getCountriesNextToOceanCount(1) == 0){
      System.out.println("It looks like it worked? :s ");
    }
    
    System.out.println(a2.getOceanInfo(1));

//    if (a2.chgHDI(1, 2009, 0.4f)){
//      System.out.println("Check out my skills brah");
//    }

//    if (a2.deleteNeighbour(9, 10)){
//      System.out.println("Skill, skillet");
//    }

    System.out.println(a2.listCountryLanguages(1));

//    if (a2.updateHeight(16, 1)){
//        System.out.println("height updated, paraguay = 7");
//    }
    if (a2.updateDB()){
      System.out.println("database updated. i hope.");
    }

    if (a2.disconnectDB()){
      System.out.println("you disconnected from the database");
    } 
  }*/  
}
