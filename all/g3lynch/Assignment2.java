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
 
        } catch (ClassNotFoundException e) {
            System.exit(1); //non-zero exit code to show that smth went wrong
        }
    }

    //Using the input parameters, establish a connection to be used for this session. Returns true if connection is sucessful
    public boolean connectDB(String URL, String username, String password){
        try {
            //connect to the database
            connection = DriverManager.getConnection(URL, username, password);
            return true;
        
        } catch (SQLException e) {
            return false;
        }
    }

    //Closes the connection. Returns true if closure was sucessful
    public boolean disconnectDB(){
        try {
            connection.close();
            return true;

        } catch (SQLException e) {
            return false;
        }
    }

    public boolean insertCountry (int cid, String name, int height, int population) {
        //if argument values are invalid, finish now
        if (height < 0 || population < 0) {
            return false;
        }
        try {
            //build the query (to check if the country already exists)
            String query = "SELECT * FROM a2.country WHERE cid = ?;";
            ps = connection.prepareStatement(query);
            ps.setInt(1, cid);
            //run the query and get its result
            rs = ps.executeQuery();
            //if the query returned data, then the country already exists
            if (rs.next()) { return false; }
            //close the query
            ps.close();
            rs.close();

            //build the query (to insert the country)
            query = "INSERT INTO a2.country VALUES (?, ?, ?, ?);";
            ps = connection.prepareStatement(query);
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            
            //run the query and return the appropriate value
            int rnum = ps.executeUpdate();
            ps.close(); //close the query
            if (rnum != 1) { return false; }    //something went wrong
            return true;    //else
        
        } catch (SQLException e) {
            return false;
        }
    }

    public int getCountriesNextToOceanCount(int oid) {
        try {
            //build the query
            String query = "SELECT count(cid) FROM a2.oceanAccess GROUP BY oid HAVING oid = ?;";
            ps = connection.prepareStatement(query);
            ps.setInt(1, oid);

            //run the query and return its result
            rs = ps.executeQuery();
            rs.next();
            int count = rs.getInt("count");
            //close the query
            ps.close();
            rs.close();
            return count;

        } catch (SQLException e) {
	        return -1;
        }
    }

    public String getOceanInfo(int oid){
        try {
            //build the query
            String query = "SELECT * FROM a2.ocean WHERE oid = ?;";
            ps = connection.prepareStatement(query);
            ps.setInt(1, oid);

            //run the query and get its result
            rs = ps.executeQuery();

            //parse and return the result
            String ret = "";
            //while loop is present to avoid throwing and error
            //when there are no results
            while (rs.next()) {
                ret += String.valueOf(rs.getInt("oid"));
                ret += ":" + rs.getString("oname");
                ret += ":" + rs.getString("depth");
            }
            //close the query
            ps.close();
            rs.close();
            return ret;
        
        } catch (SQLException e) {
            return "";
        }
    }

    public boolean chgHDI(int cid, int year, float newHDI){
        //if argument values are invalid, finish now
        //NOTE: year can theoretically be 0 or less (i.e., BCE)
        if (newHDI < 0 || newHDI > 1) {
            return false;
        }
        try {
            //build the query
            String query = "UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?;";
            ps = connection.prepareStatement(query);
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            
            //run the query and return the appropriate value
            int rnum = ps.executeUpdate();
            //close the query
            ps.close();
            if (rnum != 1) { return false; }    //something went wrong
            return true;
        
        } catch (SQLException e) {
            return false;
        }
    }

    public boolean deleteNeighbour(int c1id, int c2id){
        try {
            //build the query
            String query = "DELETE FROM a2.neighbour WHERE (country = ? AND neighbor = ?) OR (country = ? AND neighbor = ?);";
            ps = connection.prepareStatement(query);
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);
            ps.setInt(3, c2id);
            ps.setInt(4, c1id);

            //run the query and return the appropriate value
            int rnum = ps.executeUpdate();
            //close the query
            ps.close();
            if (rnum != 2) { return false; }    //something went wrong
            return true;

        } catch (SQLException e) {
            return false;
        }
    }

    public String listCountryLanguages(int cid){
	    try {
            //build the query
            String query = "SELECT * FROM a2.language JOIN a2.country USING(cid) WHERE cid = ?";
            ps = connection.prepareStatement(query);
            ps.setInt(1, cid);
            
            //run the query and get its result
            rs = ps.executeQuery();

            //parse and return the result
            String ret = "";
            while (rs.next()) {
                ret += String.valueOf(rs.getInt("lid"));
                ret += ":" + rs.getString("lname");
                float lpop = rs.getFloat("lpercentage") * rs.getInt("population");
                ret += ":" + String.valueOf(lpop);
                ret += "#";
            }
            //close the query
            ps.close();
            rs.close();
            //remove last character of the string ('#')
            if (ret.length() > 0) {
                ret = ret.substring(0, ret.length() - 1);
            }
            return ret;

        } catch (SQLException e) {
            return "";
        }
    }

    public boolean updateHeight(int cid, int decrH){
        //if argument values are invalid, finish now
        if (decrH < 0) {
            return false;
        }
        try {
            //build the query (to get original height)
            String query = "SELECT height FROM a2.country WHERE cid = ?;";
            ps = connection.prepareStatement(query);
            ps.setInt(1, cid);
            //run the query and get its result
            rs = ps.executeQuery();
            int origH = 0;
            while (rs.next()) {
                origH = rs.getInt("height");
            }
            //close the query
            ps.close();
            rs.close();

            //build the query (to update height)
            query = "UPDATE a2.country SET height = ? WHERE cid = ?;";
            ps = connection.prepareStatement(query);
            ps.setInt(1, origH - decrH);
            ps.setInt(2, cid);

            //run the query and return the appropriate value
            int rnum = ps.executeUpdate();
            //close the query
            ps.close();
            if (rnum != 1) { return false; }    //something went wrong
            return true;

        } catch (SQLException e) {
            return false;
        }
    }

    public boolean updateDB(){
        //FIXME: maybe drop the table (if it exists) before running this?
        //`DROP TABLE IF EXISTS mostPopulousCountries;`
	    try {
            //build the query (to drop the table if it exists)
            String query = "DROP TABLE IF EXISTS mostPopulousCountries;";
            sql = connection.createStatement();
            //get the result and check it
            int rnum = sql.executeUpdate(query);
            if (rnum != 0) { return false; }    //something went wrong?

            //build the query (to create table)
            query = "CREATE TABLE a2.mostPopulousCountries AS (SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY cid ASC);";

            //run the query and return the appropriate value
            rnum = sql.executeUpdate(query);
            //close the query
            sql.close();
            if (rnum != 0) { return false; }    //something went wrong?
            return true;

        } catch (SQLException e) {
            return false;
        }
    }

}
