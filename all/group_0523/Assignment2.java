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
                return true;
        }
        catch (SQLException se) {
              return false;
        }
 }

  //Closes the connection. Returns true if closure was sucessful
  public boolean disconnectDB(){
        try {
        connection.close();
        return true;
        }
        catch (SQLException se) {
         return false;
        }
}
 public boolean insertCountry (int cid, String name, int height, int population) {
        String query;
        try {
        query = "SELECT cid FROM a2.country WHERE a2.country.cid = ?";
        ps = connection.prepareStatement(query);
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        if (rs.next()) {
        return false;
        }
        else {
        query = "INSERT INTO a2.country VALUES (?, ?, ?, ?)";
        ps = connection.prepareStatement(query);
        ps.setInt(1, cid);
        ps.setString(2, name);
        ps.setInt(3, height);
        ps.setInt(4, population);
        ps.executeUpdate();
        return true;
        }
        }
        catch (SQLException se) {
        return false;
        }
  }
 public int getCountriesNextToOceanCount(int oid) {
        String query = "SELECT cid FROM a2.oceanAccess WHERE a2.oceanAccess.oid = " + oid;
        try {
        ps = connection.prepareStatement(query);
        rs = ps.executeQuery();
        int count = 0;
        while(rs.next()) {

        count++;
        }
        return count;
        }
        catch (SQLException se) {

        return -1;
        }
  }

  public String getOceanInfo(int oid){
        String query = "SELECT oid, oname, depth FROM a2.ocean WHERE a2.ocean.oid = ?";
        try {
        ps = connection.prepareStatement(query);
        ps.setInt(1, oid);
        rs = ps.executeQuery();
        rs.next();
        int id = rs.getInt("oid");
        String oname = rs.getString("oname");
        int depth = rs.getInt("depth");
        return id + ":" + oname + ":" + depth;
        }
        catch (SQLException se) {
        return "";
        }

  }
 public boolean chgHDI(int cid, int year, float newHDI){
        String query = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?";
        try {
        ps = connection.prepareStatement(query);
        ps.setFloat(1, newHDI);
        ps.setInt(2, cid);
        ps.setInt(3, year);
        ps.executeUpdate();
        return true;
        }
        catch (SQLException se) {
        return false;
        }
  }

  public boolean deleteNeighbour(int c1id, int c2id){
        String query1 = "DELETE FROM a2.neighbour WHERE a2.neighbour.country = " + c1id + " AND a2.neighbour.neighbor = " + c2id;
        String query2 = "DELETE FROM a2.neighbour WHERE a2.neighbour.country = " + c2id + " AND a2.neighbour.neighbor = " + c1id;
        try {
        ps = connection.prepareStatement(query1);
        ps.executeUpdate();
        ps = connection.prepareStatement(query2);
        ps.executeUpdate();
        return true;
        }
        catch (SQLException se) {
        return false;
        }
  }
 public String listCountryLanguages(int cid){
        String query = "SELECT lid, lname, population*lpercentage AS population FROM a2.country JOIN a2.language ON a2.country.cid = a2.language.cid WHERE a2.country.cid = ?";
        try {
        ps = connection.prepareStatement(query);
        ps.setInt(1, cid);
        rs = ps.executeQuery();
        String result = "";
        boolean first = true;
        while (rs.next()) {
        if (first) {
        result = rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getDouble("population");
        first = false;
        }
        else {
        result += "#" + rs.getInt("lid") + ":" + rs.getString("lname") + ":" + rs.getDouble("population");
        }
        }
        return result;
        }
        catch  (SQLException se) {
        return "";
        }
  }
 public boolean updateHeight(int cid, int decrH){
        String query = "SELECT height FROM a2.country WHERE a2.country.cid = " + cid;
        try {
        ps = connection.prepareStatement(query);
        rs = ps.executeQuery();
        rs.next();
        int newHeight = rs.getInt("height") - decrH;
        query = "UPDATE a2.country SET a2.country.height = " + newHeight + " WHERE a2.country.cid = " + cid;
        ps = connection.prepareStatement(query);
        ps.executeUpdate();
        return true;
        }
        catch (SQLException se) {
        return false;
        }
}

  public boolean updateDB(){
        String query = "CREATE TABLE a2.mostPopulousCountries as (SELECT cid, cname FROM a2.country WHERE a2.country.population > 1000000000 ORDER BY cid ASC)";
        try {
        ps = connection.prepareStatement(query);
        rs = ps.executeQuery();
        return true;
        }
        catch (SQLException se) {
        return false;
        }
}

}
