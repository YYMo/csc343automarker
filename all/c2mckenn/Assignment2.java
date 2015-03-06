import java.sql.*;
import java.util.ArrayList;

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
        }
    }
  
  //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try{
            connection = DriverManager.getConnection(URL, username, password);
        }
        
        catch (SQLException e) {
            return false;
         }
         return true;
    }
  
  //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try{
            if (sql != null) sql.close();
            if (ps != null) ps.close();
            if (rs != null) rs.close();
            connection.close();
        }
        catch (SQLException e) {
            return false;
        }
        return true;
    }
    
    private void setPath(){
        try {
            sql = connection.createStatement();
            sql.executeQuery("SET search_path TO A2;");
        } catch (SQLException e) {
        }
    }
    
    public boolean insertCountry (int cid, String name, int height, int population) {
        
        setPath();
        ArrayList<Integer> cids = new ArrayList<Integer>();
        
        try {
            sql = connection.createStatement();
            rs = sql.executeQuery("SELECT cid FROM a2.country;");
            
            while (rs.next()) {
                cids.add(rs.getInt("cid"));
            }
            
            if (cids.contains(cid)) {
                return false;
            }
            
            ps = connection.prepareStatement("INSERT INTO a2.country VALUES (?, ?, ?, ?)");
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            
            ps.executeUpdate();
            connection.commit();
            
            if (sql != null) sql.close();
            if (ps != null) ps.close();
            if (rs != null) rs.close();
        
        }
        catch (SQLException e) {
            return false;
        }
        
        return true;
    }
  
    public int getCountriesNextToOceanCount(int oid) {
        
        setPath();
        int countries = 0;
        
        try {
            ps = connection.prepareStatement("SELECT DISTINCT cid FROM a2.oceanAccess WHERE oid = ?");
            ps.setInt(1, oid);
            
            rs = ps.executeQuery();
            
            while (rs.next()) {
                countries++;
            }
            
            if (ps != null) ps.close();
            if (rs != null) rs.close();
        
        }
        catch (SQLException e) {
            return -1;
        }
        
        return countries;
    }
   
    public String getOceanInfo(int oid){
        setPath();
        int id, depth;
        String oname;
        String info = "";
        
        
        try {
            ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?");
            ps.setInt(1, oid);
            
            rs = ps.executeQuery();
            
            while(rs.next()){
                id = rs.getInt("oid");
                oname = rs.getString("oname");
                depth = rs.getInt("depth");
                info = id + ":" + oname + ":" + depth;
            }
            
            if (ps != null) ps.close();
            if (rs != null) rs.close();
            
        }
        catch (SQLException e) {
            return "";
        }
 
        return info;
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        setPath();
        
        try {
            ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE (cid = ?) AND (year = ?)");
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            
            ps.executeUpdate();
            connection.commit();
            
            if (ps != null) ps.close();
        }
        catch (SQLException e) {
            return false;
        }
        
        return true;
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        setPath();
        
        try {
            ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE ((country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?))");
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);
            
            ps.executeUpdate();
            connection.commit();
            
            if (ps != null) ps.close();
        }
        
        catch (SQLException e) {
            return false;
        }
        
        return true;
    }
    
    public String listCountryLanguages(int cid) {
        setPath();
        int lid, population;
        String lname;
        
        String result = "";
        try {
            ps = connection.prepareStatement("SELECT lid, lname, (lpercentage * population) as population FROM language, country WHERE (country.cid = ? AND language.cid = ?)");
            ps.setInt(1, cid);
            ps.setInt(2, cid);
            
            rs = ps.executeQuery();
            
            while(rs.next()){
                lid = rs.getInt("lid");
                lname = rs.getString("lname");
                population = rs.getInt("population");
                
                result = result + lid + ":" + lname + ":" + population + "#";
            }
            
            if (ps != null) ps.close();
            if (rs != null) rs.close();
        }
        
        catch (SQLException e) {
            return "";
        }
        return result;
    }
  
    public boolean updateHeight(int cid, int decrH){
        setPath();
        try {
            ps = connection.prepareStatement("UPDATE country SET height = height - ? WHERE cid = ?");
            ps.setInt(1, decrH);
            ps.setInt(2, cid);
            
            ps.executeUpdate();
            connection.commit();
            
            if (ps != null) ps.close();
        }
        
        catch (SQLException e) {
            return false;
        }

        return true;
    }
    
    public boolean updateDB(){
        setPath();
        try{
            sql = connection.createStatement();
            rs = sql.executeQuery("SELECT cid, cname FROM a2.country WHERE population > 100000000;");
            sql.close();
            
            sql = connection.createStatement();
            sql.executeQuery("CREATE TABLE mostPopulousCountries (cid INTEGER REFERENCES a2.country(cid), cname VARCHAR(20) NOT NULL, PRIMARY KEY(cid));");
            sql.close();
            
            while(rs.next()){
                ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (cid, cname) VALUES (?, ?)");
                ps.setInt(1, rs.getInt("cid"));
                ps.setString(2, rs.getString("cname"));
                
                ps.executeUpdate();
                ps.close();
            }
            connection.commit();
        }
        
        catch (SQLException e) {
            return false;
        }
        
        return true;
    }
    
}
