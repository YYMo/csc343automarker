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

    // queryString for the query statements
    String queryString;
    
    //CONSTRUCTOR
    //Identifies the postgreSQL driver using Class.forName method.
    Assignment2(){
        try {
            Class.forName("org.postgresql.Driver");
        }
        catch (ClassNotFoundException e){
            System.out.println("Failed to find the JDBC driver");
        }
    }
    
    // Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            connection = DriverManager.getConnection(URL, username, password);
            return true;
        }
        catch (SQLException se) {
            return false;
        }
    }
    
    // Closes the connection. Returns true if closure was successful
    public boolean disconnectDB(){
        try {
            connection.close();
            return true;
        }
        catch (SQLException se){
            return false;
        }
    }
    
    // Inserts a row into the country table. cid is the name of the country, name is the name of the country, height is the highest elevation point and population is the population of the newly inserted country. You have to check if the country with id cid exists. Returns true if the insertion was successful, false otherwise.
    public boolean insertCountry (int cid, String name, int height, int population) {
        try{
            // First check if cid exists
            queryString = "SELECT * FROM a2.country WHERE cid = ?";
            
            ps = connection.prepareStatement(queryString);
            ps.setInt(1, cid);
            
            rs = ps.executeQuery();
            if (rs.next()){
                return false;
            }
            
            
            // Give that the cid does not exist, insert cid into country
            queryString = "INSERT INTO a2.country(cid, cname, height, population) VALUES (?, ?, ?, ?)";
            
            ps = connection.prepareStatement(queryString);
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            
            int execute = ps.executeUpdate();
            
            ps.close();
            rs.close();
            
            return true;
        }
        catch (SQLException e){
            System.err.println("SQL Exception Error: " + "<Message>");
            return false;
        }
    }

    // Returns the number of countries in table oceanAccess that are located next to the ocean with id oid. Returns -1 if an error occurs.
    public int getCountriesNextToOceanCount(int oid) {
        try {
            queryString = "SELECT COUNT(*) FROM a2.oceanAccess WHERE oid = ?";
            
            ps = connection.prepareStatement(queryString);
            
            ps.setInt(1, oid);
            rs = ps.executeQuery();
            
            rs.next();
            int answer = rs.getInt("count");
            
            ps.close();
            rs.close();
            
            return answer;
        }
        catch (SQLException e){
            return -1;
        }
    }


    // Returns a string with the information of an ocean with id oid. The output is “oid:oname:depth”. Returns an empty string “” if the ocean does not exist.
    public String getOceanInfo(int oid) {
        try {
            queryString = "SELECT * FROM a2.ocean WHERE oid = ?";
            
            ps = connection.prepareStatement(queryString);
            ps.setInt(1, oid);
            
            rs = ps.executeQuery();
            
            rs.next();
            String answer_oid = Integer.toString(rs.getInt("oid"));
            String answer_oname = rs.getString("oname");
            String answer_depth = Integer.toString(rs.getInt("depth"));
            
            ps.close();
            rs.close();
            
            return answer_oid + ":" + answer_oname + ":" + answer_depth;
        }
        catch (SQLException se){
            return "";
        }
    }
    
    // Changes the HDI value of the country cid for the year year to the HDI value supplied (newHDI). Returns true if the change was successful, false otherwise.
    public boolean chgHDI(int cid, int year, float newHDI){
        try {
            queryString = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? and year = ?";

            ps = connection.prepareStatement(queryString);

            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            rs = ps.executeQuery();

            ps.close();
            rs.close();

            return true;
            
        }
        catch (SQLException se){
            return false;
        }
    }

    // Deletes the neighboring relation between two countries. Returns true if the deletion was successful, false otherwise. You can assume that the neighboring relation to be deleted exists in the database. Remember that if c2 is a neighbor of c1, c1 is also a neighbour of c2.
    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            queryString = "DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?)";
            
            ps = connection.prepareStatement(queryString);
            
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);

            ps.executeUpdate();

            ps.close();
            
            return true;
        }
        catch (SQLException se){

            return false;
        }
    }
    
    // Returns a string with all the languages that are spoken in the country with id cid.
    public String listCountryLanguages(int cid){
        try {
            // First we need the population of the country with id cid
            queryString = "SELECT population FROM a2.country WHERE cid = ?";
            
            ps = connection.prepareStatement(queryString);
            ps.setInt(1, cid);
            
            rs = ps.executeQuery();
            rs.next();
            int population  = rs.getInt("population");
            
            // Given the correct population, we can finally calculate the number of people in a country that speak the language
            queryString = "SELECT * FROM a2.language WHERE cid = ?";
            
            ps = connection.prepareStatement(queryString);
            ps.setInt(1, cid);
            
            rs = ps.executeQuery();
            String answer = "";
            while  (rs.next()) {
                String answer_lid = Integer.toString(rs.getInt("lid"));
                String answer_lname = rs.getString("lname");
                String answer_population = String.valueOf((rs.getDouble("lpercentage") * population));
                answer += answer_lid + ":" + answer_lname + ":" + answer_population + "#";
            }

            ps.close();
            rs.close();

            return answer;
        }
        catch (SQLException se) {
            return "";
        }
    }

    // Decreases the height of the country with id cid. (A decrease might happen due to natural erosion.) Returns true if the update was successful, false otherwise.
    public boolean updateHeight(int cid, int decrH){
        try {
            queryString = "UPDATE a2.country SET height = ? WHERE cid = ?";
            
            ps = connection.prepareStatement(queryString);
            
            ps.setInt(1, decrH);
            ps.setInt(2, cid);
            rs = ps.executeQuery();
            
            ps.close();
            rs.close();
            
            return true;
        }
        catch (SQLException se){
            return false;
        }
    }

    //Create a table containing all the countries which have a population over 100 million.
    public boolean updateDB(){
        try {
            queryString = "CREATE TABLE mostPopulousCountries AS SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC";
            
            ps = connection.prepareStatement(queryString);
            
            rs = ps.executeQuery();
            
            ps.close();
            rs.close();
            
            return true;
        }
        catch (SQLException se){
            return false;
        }
    }
}