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
        }
    }
  
    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try {
            connection.close();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public boolean insertCountry (int cid, String name, int height, int population) {
        try {
            // Check if country with cid exists before inserting
            String searchCid = "SELECT cid FROM a2.country WHERE cid=?;";
            ps = connection.prepareStatement(searchCid);
            ps.setLong(1, cid);
            rs = ps.executeQuery();
            if (rs.next()){
                return false;
            }

            String insert = "INSERT INTO a2.country VALUES(?, ?, ?, ?);";
            ps = connection.prepareStatement(insert);
            ps.setLong(1, cid);
            ps.setString(2, name);
            ps.setLong(3, height);
            ps.setLong(4, population);
            ps.executeUpdate();
        } catch (SQLException e) {
            return false;
        }
        return true;
    }

    public int getCountriesNextToOceanCount(int oid) {
        try {
            // Check if ocean with oid exists before
            String searchOid = "SELECT oid FROM a2.ocean WHERE oid=?;";
            ps = connection.prepareStatement(searchOid);
            ps.setLong(1, oid);
            rs = ps.executeQuery();
            if (!rs.next()){
                return -1;
            }

            int countryNum;
            String searchCountryNum = "SELECT count(*) FROM a2.oceanaccess WHERE oid=?;";
            ps = connection.prepareStatement(searchCountryNum);
            ps.setLong(1, oid);
            rs = ps.executeQuery();
            if (rs.next()) {
                countryNum = rs.getInt(1);
            } else {
                countryNum = -1;
            }
            return countryNum;
        } catch (SQLException e) {
            return -1;
        }
    }

    public String getOceanInfo(int oid){
        try{
            String searchOcean = "SELECT oid, oname, depth FROM a2.ocean WHERE oid=?;";
            ps = connection.prepareStatement(searchOcean);
            ps.setLong(1, oid);
            rs = ps.executeQuery();
            if (rs.next()) {
                String oidResult = rs.getString("oid");
                String onameResult = rs.getString("oname");
                String depthResult = rs.getString("depth");
                return oidResult + ":" + onameResult + ":" + depthResult;
            } else {
                return "";
            }
        } catch (SQLException e) {
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        try{
            String changeHDI = "UPDATE a2.hdi SET hdi_score=? WHERE cid=? AND year=?;";
            ps = connection.prepareStatement(changeHDI);
            ps.setLong(2, cid);
            ps.setLong(3, year);
            ps.setFloat(1, newHDI);
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        try{
            String del ="DELETE FROM a2.neighbour WHERE (country=? AND neighbor=?) OR (country=? AND neighbor=?);";
            ps = connection.prepareStatement(del);
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public String listCountryLanguages(int cid){
        try{
            String listLanguage ="SELECT lid, lname, lpercentage*population as speakers FROM a2.language, a2.country WHERE language.cid=country.cid AND language.cid=? ORDER BY speakers;";
            ps = connection.prepareStatement(listLanguage);
            ps.setInt(1, cid);
            rs = ps.executeQuery();
            String results = "";
            while(rs.next()) {
                results += "#" + rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getDouble("speakers");
            }
            // Substring to get rid of first '#'
            if (results.length() > 0) {
                return results.substring(1);
            } else {
                return "";
            }
        } catch (SQLException e) {
            return "";
        }
    }

    public boolean updateHeight(int cid, int decrH){
        try{
            // Check if country with cid exists before reducing height
            String searchCid = "SELECT height FROM a2.country WHERE cid=?;";
            ps = connection.prepareStatement(searchCid);
            ps.setLong(1, cid);
            rs = ps.executeQuery();
            int curHeight;
            if (rs.next()){
                curHeight = rs.getInt(1);
            } else {
                return false;
            }
            
            String reduceHeight = "UPDATE a2.country SET height=? WHERE cid=?";
            ps = connection.prepareStatement(reduceHeight);
            ps.setLong(1,  curHeight - decrH);
            ps.setLong(2, cid);
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean updateDB(){
        try{
            String deleteTable = "DROP TABLE IF EXISTS a2.mostPopulousCountries;";
            ps = connection.prepareStatement(deleteTable);
            ps.executeUpdate();
            String createTable = "SELECT cid, cname INTO a2.mostPopulousCountries "
                                 + "FROM a2.country "
                                 + "WHERE population>100000000;";
            ps = connection.prepareStatement(createTable);
            ps.executeUpdate();
            return true;
        } catch (SQLException e) {
            return false;
        }    
    }

}
