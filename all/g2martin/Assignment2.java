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
            System.out.println("Failed to load the driver.");
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }
  
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try {
            connection.close();
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }
    
    public boolean insertCountry (int cid, String name, int height, int population) {
	    /*
    	An example for executing a query:
	    String queryString = "SELECT pingas,egg FROM snooping WHERE sonic=True";
    	ps = connection.prepareStatement(queryString);
    	rs = ps.executeQuery();
    	while(rs.next()) {
    		String name = rs.getString("pingas");
    		int egg = rs.getInt("egg");
    	}

    	Or, with prepared values:
    	ps = connection.prepareStatement("INSERT INTO snooping(pingas,egg) VALUES (?,?)");
    	ps.setString(1, "As usual, I see");
    	ps.setString(2, "1");
    	ps.executeUpdate();
    	*/

        try {
            ps = connection.prepareStatement(
                    "SELECT cid FROM a2.country WHERE cid=?");
            ps.setInt(1, cid);
            rs = ps.executeQuery();
            if(!rs.next()) {
                ps = connection.prepareStatement(
                        "INSERT INTO a2.country(cid,cname,height,population)"
                        + " VALUES (?,?,?,?)");
                ps.setInt(1, cid);
                ps.setString(2, name);
                ps.setInt(3, height);
                ps.setInt(4, population);
                ps.executeUpdate();

                return true;
            } else {
                return false;
            }
        }
        catch (SQLException e) {
            return false;
        }
    }
  
    public int getCountriesNextToOceanCount(int oid) {
        try {
            ps = connection.prepareStatement(
                    "SELECT COUNT(DISTINCT oa.cid)"
                    + " FROM a2.oceanAccess oa"
                    + " WHERE oa.oid=?");
            
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            
            if(rs.next()) {
                return rs.getInt(1);
            } else {
                return -1;
            }
        }
        catch (SQLException e) {
            return -1;
        }
    }
   
    public String getOceanInfo(int oid){
        try {
            ps = connection.prepareStatement(
                    "SELECT *"
                    + " FROM a2.ocean o"
                    + " WHERE o.oid=?");
            
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            
            if(rs.next()) {
                return rs.getString("oid") + ":" + rs.getString("oname") + ":"
                        + rs.getString("depth");
            } else {
                return "";
            }
        }
        catch (SQLException e) {
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        try {
            ps = connection.prepareStatement(
                    "UPDATE a2.hdi"
                    + " SET hdi_score=?"
                    + " WHERE cid=?"
                    + " AND year=?");
            
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            int result = ps.executeUpdate();
            
            if(result == 0) {
                return false;
            } else {
                return true;
            }
        }
        catch (SQLException e) {
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            ps = connection.prepareStatement(
                    "DELETE FROM a2.neighbour"
                    + " WHERE (country=? AND neighbor=?)"
                    + " OR (country=? AND neighbor=?)");
            
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);
            int result = ps.executeUpdate();
            
            if(result == 0) {
                return false;
            } else {
                return true;
            }
        }
        catch (SQLException e) {
            return false;
        }
    }
  
    public String listCountryLanguages(int cid){
        try {
            ps = connection.prepareStatement(
                    "SELECT l.lid, l.lname, ((l.lpercentage / 100) * c.population) AS tot_population"
                    + " FROM a2.language l"
                    + " LEFT JOIN a2.country c ON c.cid=l.cid"
                    + " WHERE l.cid=?"
                    + " ORDER BY tot_population");
            
            ps.setInt(1, cid);
            rs = ps.executeQuery();
            
            String resultString = "";
            int i = 1;
            while(rs.next() && !rs.isLast()) {
                resultString += rs.getString("lid")
                        + ":" + rs.getString("lname")
                        + ":" + rs.getString("tot_population")
                        + "#"
                        ;
                i++;
            }
            
            resultString += rs.getString("lid")
                        + ":" + rs.getString("lname")
                        + ":" + rs.getString("tot_population")
                        ;
            
            return resultString;
        }
        catch (SQLException e) {
            return "";
        }
    }
  
    public boolean updateHeight(int cid, int decrH){
        try {
            ps = connection.prepareStatement(
                    "UPDATE a2.country"
                    + " SET height=height - ?"
                    + " WHERE cid=?");
            
            ps.setInt(1, decrH);
            ps.setInt(2, cid);
            int result = ps.executeUpdate();
            
            if(result == 0) {
                return false;
            } else {
                return true;
            }
        }
        catch (SQLException e) {
            return false;
        }
    }
    
    public boolean updateDB(){
        try {
            ps = connection.prepareStatement(
                    "CREATE TABLE a2.mostPopulousCountries"
                    + " AS ("
                    + "     SELECT c.cid, c.cname"
                    + "     FROM a2.country c"
                    + "     WHERE c.population > 100000000"
                    + "     ORDER BY c.cid ASC"
                    + " )"); // Shortcuts ftw
            
            ps.executeUpdate();
            
            return true;
        }
        catch (SQLException e) {
            return false;
        }
    }

}
