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
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e){
            return;
        }
    }
    
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
            if (connection == null) return false;
            return true;
        }
        catch (SQLException e){
            return false;
        }
    }
    
    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try {
            connection.close();
            return true;
        }
        catch (SQLException e){
            return false;
        }
    }
      
    public boolean insertCountry (int cid, String name, int height, int population) {
        try {
            String sqltext = "INSERT INTO a2.country VALUES (?,?,?,?)";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (SQLException e){
            return false;
        }
    }
    
    public int getCountriesNextToOceanCount(int oid) {
        try {
            String sqltext = "SELECT count(cid) from a2.oceanAccess where oid=(?)";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            if (rs.next()){
                int retVal = rs.getInt("count");
                rs.close();
                ps.close();
                return retVal;
            }
            else {
                rs.close();
                ps.close();
                return 0;
            }
        }
        catch (SQLException e){
            return -1;
        }
    }
     
    public String getOceanInfo(int oid){
        try {
            String sqltext = "SELECT * from a2.ocean where oid=(?)";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            String output = "";
            if (rs.next()){
                output += rs.getString("oid") + ":" + rs.getString("oname") + ":" + rs.getString("depth");
                rs.close();
                ps.close();
                return output;
            }
            else {
                rs.close();
                ps.close();
                return "";
            }
        }
        catch (SQLException e){
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        try {
            String sqltext = "UPDATE a2.hdi SET hdi_score=(?) where cid=(?) and year=(?)";
            ps = connection.prepareStatement(sqltext);
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (SQLException e){
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            String sqltext = "DELETE from a2.neighbour where country=(?) and neighbor=(?)";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, c2id);
            ps.setInt(2, c1id);
            ps.executeUpdate();
            ps.close();
            sqltext = "DELETE from a2.neighbour where country=(?) and neighbor=(?)";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (SQLException e){
            return false;
        }
    }
    
    public String listCountryLanguages(int cid){
        try {
            String sqltext = "SELECT lid, lname, lpercentage*population AS lpop from a2.language NATURAL JOIN a2.country where cid=(?) order by lpercentage*population";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, cid);
            rs = ps.executeQuery();
            String output = "";
            while (rs.next()){
                output += rs.getString("lid") + ":" + rs.getString("lname") + ":" + rs.getString("lpop") + "#";
            }
            output = output.substring(0, output.length()-1);
            rs.close();
            ps.close();
            return output;
        }
        catch (SQLException e){
            return "";
        }
    }

    public boolean updateHeight(int cid, int decrH){
        try {
            String sqltext = "SELECT height from a2.country where cid=(?)";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, cid);
            rs = ps.executeQuery();
            int height;
            if (rs.next()){
                height = rs.getInt("height");
                height -= decrH;
            }
            else {
                rs.close();
                ps.close();
                return false;
            }
            ps.close();

            sqltext = "UPDATE a2.country SET height=(?) where cid=(?)";
            ps = connection.prepareStatement(sqltext);
            ps.setInt(1, height);
            ps.setInt(2, cid);
            ps.executeUpdate();
            rs.close();
            ps.close();
            return true;
        }
        catch (SQLException e){
            return false;
        }
    }
      
    public boolean updateDB(){
        try {
            String sqltext = "CREATE TABLE a2.mostPopulousCountries (" +
                             "cid       INTEGER," +
                             "cname     VARCHAR(20))";
            ps = connection.prepareStatement(sqltext);
            ps.executeUpdate();

            ps.close();

            sqltext = "INSERT INTO a2.mostPopulousCountries (" +
                      "SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY population ASC)";
            ps = connection.prepareStatement(sqltext);
            ps.executeUpdate();
            ps.close();
            return true;
        }
        catch (SQLException e){
            return false;
        }
    }
    
}
