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
        catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
  
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
  
  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
      try {
        connection.close();
        return true;
      }
      catch (Exception e) {
        return false;
      }
  }
    
    public boolean insertCountry (int cid, String name, int height, int population) {
        try {
            ps = connection.prepareStatement(
                "INSERT INTO country VALUES(?, ?, ?, ?)");
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
  
    public int getCountriesNextToOceanCount(int oid) {
        try {
            String query = "SELECT DESTINCT cid from oceanaccess";
            ps = connection.prepareStatement(query);
            rs = ps.executeQuery();
            int count = 0;
            while(rs.next())
                count++;
            ps.close();
            rs.close();
            return count;
        }
        catch (Exception e) {
            return -1; 
        }
    }

    public String getOceanInfo(int oid){
        try {
            ps = connection.prepareStatement(
                "SELECT * FROM ocean WHERE oid=?");
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            String result = "";
            if(rs.next()) {
                result = rs.getInt("oid") + ":" +
                       rs.getString("oname") + ":" +
			rs.getInt("depth");
            }
            rs.close();
            ps.close();
            return result;
        }
        catch (Exception e) {
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        try {
            ps = 
            connection.prepareStatement("UPDATE hdi SET hdi_score=?" +
                                        " WHERE cid=? AND year=?");
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            ps = connection.prepareStatement(
                "DELETE FROM neighbour WHERE (country=? AND" +
                " neighbor=?) OR (country=? AND neighbor=?)");
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }

    public String listCountryLanguages(int cid){
        try {
            String q = "SELECT a.lid, a.lname, " + 
                       "round(b.population * a.lpercentage /100) as" +
                       " population FROM language a, country b" +
                       " WHERE b.cid=a.cid AND a.cid=?" +
                       " ORDER BY population";
            ps = connection.prepareStatement(q);
            ps.setInt(1, cid);
            String result = "";
            rs = ps.executeQuery();
            if (rs.next()) {
                result += rs.getInt("lid") + ":";
                result += rs.getString("lname") +":";
                result += rs.getInt("population");
                while (rs.next()) {
                    result += "#";
                    result += rs.getInt("lid") + ":";
                    result += rs.getString("lname") +":";
                    result += rs.getInt("population");
                }
            }
            rs.close();
            ps.close();
            return result;
        }
        catch (Exception e) {
            return "";
        }
    }
  
    public boolean updateHeight(int cid, int decrH){
        try {
            ps = connection.prepareStatement(
                "UPDATE country SET height=height-? WHERE cid=?");
            ps.setInt(1, decrH);
            ps.setInt(2, cid);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
    
    public boolean updateDB(){
        try {
            String create = "CREATE TABLE mostPopulousCountries ( " +
                            "cid INTEGER, " + "cname VARCHAR(20), " + 
                            "PRIMARY KEY (cid))";
            ps = connection.prepareStatement(create);
            ps.executeUpdate();
            ps.close();
            String insert = "INSERT INTO mostPopulousCountries ( " + 
                           "SELECT cid, cname FROM country WHERE" +
                           " population>100000000 ORDER BY cid ASC)";
            ps = connection.prepareStatement(insert);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (Exception e) {
            return false;
        }
    }
  
}
