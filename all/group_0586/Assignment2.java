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
      System.exit(1);
    }
  }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
  public boolean connectDB(String URL, String username, String password){
    try {
      connection = DriverManager.getConnection(URL, username, password);
    } catch (SQLException e) {
      return false;
    }
    if (connection != null) {
      return true;
    } else {
      return false;
    }
  }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
    try {
      if(connection != null) {
        connection.close();
        return true;
      } else {
        return false;
      }
    } catch (SQLException e) {
      return false;
    }
  }
    
  public boolean insertCountry (int cid, String name, int height, int population) {


        // String query = "INSERT INTO a2.country (cid,cname,height,population) SELECT ?,?,?,? WHERE NOT EXISTS (SELECT cid FROM a2.country WHERE cid = ?)";
        String query = "INSERT INTO a2.country (cid,cname,height,population) SELECT ?,?,?,? ";
        int result;

        try{
            ps = connection.prepareStatement(query);
            ps.setInt(1,cid);
            ps.setString(2,name);
            ps.setInt(3,height);
            ps.setInt(4,population);
            // ps.setInt(5,cid);
            result = ps.executeUpdate();
        }
          catch (SQLException sq) {
            return false;
          }

        if (result == 1) {
          return true;
        }
        else {
          return false;
        }
  }

  public int getCountriesNextToOceanCount(int oid) {
        String query = "select * from a2.oceanAccess where oid = ? ";
        int result = 0;
        try{
            ps = connection.prepareStatement(query);
            ps.setInt(1,oid);
            rs = ps.executeQuery();

            while (rs.next()) {
                result++;
                
            }
            return result;
        }
        catch (SQLException sq) {
            return -1;
        }
        
  }

  public String getOceanInfo(int oid){

     String query = "select * from a2.ocean where oid = ? ";

    try {
        ps = connection.prepareStatement(query);
        ps.setInt(1,oid);
        rs = ps.executeQuery();
        if (!rs.next()){
            return "";
        }
        else {

            String id = String.valueOf(rs.getInt("oid"));
            String oname = rs.getString("oname");
            String depth = String.valueOf(rs.getInt("depth"));
            return id + ":" + oname + ":" + depth;
        }
    }
    catch (SQLException sq) {
          return "";
    }

  }

  public boolean chgHDI(int cid, int year, float newHDI){
        String query = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? and year = ?";
        int result;
        try{
            ps = connection.prepareStatement(query);

            ps.setFloat(1,newHDI);
            ps.setInt(2,cid);
            ps.setInt(3,year);
           
            result = ps.executeUpdate();
        }
        catch (SQLException sq) {
            return false;
        }

        if (result == 1) {
          return true;
        }
        else {
          return false;
        }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
        String query = "DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ?) or (country = ? AND neighbor = ?)";
        int result;
        try{
            ps = connection.prepareStatement(query);

            
            ps.setInt(1,c1id);
            ps.setInt(2,c2id);
           
            ps.setInt(3,c2id);
            ps.setInt(4,c1id);
           
           
            result = ps.executeUpdate();
        }
        catch (SQLException sq) {
            return false;
        }

        if (result == 2) {
          return true;
        }
        else {
          return false;
        }
     
  }
  

  public String listCountryLanguages(int cid){
    String query = "select lid, lname, lpercentage, population from a2.language, a2.country where language.cid = country.cid and language.cid = ? and country.cid = ?";
    String answer = "";
    try {
        ps = connection.prepareStatement(query);
        ps.setInt(1,cid);
        ps.setInt(2,cid);
        
        rs = ps.executeQuery();

    
        while (rs.next()){
              String id = String.valueOf(rs.getInt("lid"));
              String lname = rs.getString("lname");
              int percentage = rs.getInt("lpercentage");
              int pop = rs.getInt("population");
              int result = percentage * pop / 100;
              
              String popu = String.valueOf(result);

              answer += id + ":" + lname + ":" + popu + "#";
        }
        return answer;
        
    }
    catch (SQLException sq) {
          return "";
    }
  }

  public boolean updateHeight(int cid, int decrH){
        String query = "UPDATE a2.country SET height = ? WHERE cid = ?";
        int result;
        try{
            ps = connection.prepareStatement(query);
  
            ps.setInt(1,decrH);
            ps.setInt(2,cid);
                    
            result = ps.executeUpdate();
        }
        catch (SQLException sq) {
            return false;
        }

        if (result == 1) {
          return true;
        }
        else {
          return false;
        }
  }

  public boolean updateDB(){
        String query = "CREATE TABLE IF NOT EXISTS a2.mostPopulousCountries (cid INTEGER PRIMARY KEY, cname VARCHAR(20))";
        int result;
        try{
            ps = connection.prepareStatement(query);
                    
            result = ps.executeUpdate();

            query = "insert into a2.mostPopulousCountries (select cid, cname from a2.country where population > 100000000 ORDER BY cid ASC)";
            ps = connection.prepareStatement(query);
            result = ps.executeUpdate();
        }
        catch (SQLException sq) {
            return false;
        }

        return true;   
  }


}