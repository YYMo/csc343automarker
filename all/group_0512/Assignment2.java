import java.sql.*;

/**
 * Allows several interactions with a postgreSQL database.
 */
public class Assignment2 {

    // A connection to the database
    Connection connection;

    // Statement to run queries
    Statement sql;

    // Prepared Statement
    PreparedStatement ps;

    // Resultset for the query
    ResultSet rs;

    // CONSTRUCTOR
    Assignment2() {
        try {
            Class.forName("org.postgresql.Driver");
        } catch (ClassNotFoundException cnfe) {
            // Do nothing.
        }
    }

    // Using the input parameters, establish a connection to be used for this session.
    // Returns true if connection is successful
    public boolean connectDB(String URL, String username, String password) {

        try {
            connection = DriverManager.getConnection(
                    URL,
                    username,
                    password);
        } catch (SQLException ex) {
            return false;
        }

        return true;
    }

    //Closes the connection. Returns true if closure was successful
    public boolean disconnectDB() {
        try {
            if (connection != null) {
                connection.close();
			}

            if (rs != null) {
	            rs.close();
            }

            if (ps != null) {
	            ps.close();
            }

            if (sql != null) {
	            sql.close();
            }

        } catch (SQLException ex) {
            return false;
        }
        return true;
    }

    /**
     * Inserts a row into the country table. cid is the name of the country, name is the
     * name of the country, height is the highest elevation point and population is the
     * population of the newly inserted country. You have to check if the country with id
     * cid exists. Returns true if the insertion was successful, false otherwise.
     */
     public boolean insertCountry (int cid, String name, int height, int population) {
        int numUpdate;

        try {

            // Not necessary to check for duplicates. The responsibility is given
            // to the DBMS. Ref: CSC343 Piazza post @372, Manos' answer.
            ps = connection.prepareStatement("INSERT INTO a2.country VALUES (?, ?, ?, ?);");
            ps.setInt(1, cid);
            ps.setString(2, name);
            ps.setInt(3, height);
            ps.setInt(4, population);
            numUpdate = ps.executeUpdate();

            if (numUpdate != 1) {
                return false;
            }

        } catch (SQLException ex) {
            return false;
        }

        return true;
    }

    /**
     * Returns the number of countries in table "oceanAccess" that are located next
     * to the ocean with id oid. Returns -1 if an error occurs.
     */
    public int getCountriesNextToOceanCount(int oid) {
        int count = 0;

        try {
            ps = connection.prepareStatement("SELECT cid FROM a2.oceanaccess WHERE oid = ?;");
            ps.setInt(1, oid);
            rs = ps.executeQuery();

            // Count number of rows in rs.
            while (rs.next()) {
                count++;
            }

        } catch (SQLException ex) {
            return -1;
        }


        return count;
    }

    /**
     *  Returns a string with the information of an ocean with id oid. The output is
     *  "oid:oname:depth". Returns an empty string "" if the ocean does not exist.
     */
    public String getOceanInfo(int oid) {
        String result;

        try {
            ps = connection.prepareStatement("SELECT * FROM a2.ocean WHERE oid = ?;");
            ps.setInt(1, oid);

            rs = ps.executeQuery();
            rs.next();

            result = rs.getInt(1) + ":";
            result = result + rs.getString(2) + ":";
            result = result + rs.getInt(3);
        } catch (SQLException ex) {
            return "";
        }

        return result;
    }

    /**
     * Changes the HDI value of the country cid for the year year to the HDI value
     * supplied (newHDI). Returns true if the change was successful, false otherwise.
     */
    public boolean chgHDI(int cid, int year, float newHDI) {
        int numCount;

        try {
            ps = connection.prepareStatement("UPDATE a2.hdi SET hdi_score = ? WHERE cid = ? AND year = ?;");
            ps.setFloat(1, newHDI);
            ps.setInt(2, cid);
            ps.setInt(3, year);
            numCount = ps.executeUpdate();

            if (numCount != 1) {
                return false;
            }

        } catch (SQLException ex) {
            return false;
        }
        return true;
    }

    /**
     * Deletes the neighboring relation between two countries. Returns true if the
     * deletion was successful, false otherwise. You can assume that the neighboring
     * relation to be deleted exists in the database. Remember that if c2 is a neighbor of
     * c1, c1 is also a neighbour of c2.
     */
     public boolean deleteNeighbour(int c1id, int c2id) {
        int numCount;

        try {
	    // Delete the first reference.
            ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? and neighbor = ?;");
            ps.setInt(1, c1id);
            ps.setInt(2, c2id);

            numCount = ps.executeUpdate();

            if (numCount != 1) {
                return false;
            }

	    // Delete the second reference.
            ps = connection.prepareStatement("DELETE FROM a2.neighbour WHERE country = ? and neighbor = ?;");
            ps.setInt(2, c1id);
            ps.setInt(1, c2id);

            numCount = ps.executeUpdate();

            if (numCount != 1) {
                return false;
            }

        } catch (SQLException ex) {
            return false;
        }

        return true;
    }

    /** Returns a string with all the languages that are spoken in the country with id cid.
     * The list of languages should follow the contiguous format described above, and
     * contain the following attributes in the order shown: (NOTE: before creating the
     * string order your results by population).
     *         "l1id:l1lname:l1population#l2id:l2lname:l2population#... "
     * where:
     *       * lid is the id of the language.
     *       * lname is name of the country.
     *       * population is the number of people in a country that speak the language,
     * note that you will need to compute this number, as it is not readily
     * available in the database.
     * Returns an empty string "" if the country does not exist.
     */
     public String listCountryLanguages(int cid) {
        String result = "";
        try {

            // Piazza: @373 requests us to not order by ASC or DESC. 
            // Piazza: @392 clarifies that lname should represent language name, not country name.
            ps = connection.prepareStatement("SELECT lid, lname, lpercentage * population as perc FROM " +
                                             "a2.language JOIN a2.country on language.cid = country.cid WHERE " +
                                             "language.cid = ? ORDER BY perc;");
            ps.setInt(1, cid);
            rs = ps.executeQuery();

            while (rs.next()) {
                result = result + rs.getInt(1) + ":";
                result = result + rs.getString(2) + ":";
                result = result + rs.getFloat(3);
	        
		        // Checks to see if the row is the last row, as we do not want to add a '#' after the last row.
                if (!rs.isLast()) {
                    result = result + "#";
                }

            }

        } catch (Exception ex) {
            return "";
        }

        return result;
    }

    /**
     * Decreases the height of the country with id cid. (A decrease might happen due to natural erosion.)
     * Returns true if the update was successful, false otherwise.
     */
    public boolean updateHeight(int cid, int decrH) {
        int height;
        int numCount;

        try {
            ps = connection.prepareStatement("SELECT * FROM a2.country WHERE cid = ?;");
            ps.setInt(1, cid);

            rs = ps.executeQuery();
            rs.next();
			
            height = rs.getInt(3);
            height = height - decrH;
	
	        // Update the new height.
            ps = connection.prepareStatement("UPDATE a2.country SET height = ? WHERE cid = ?;");
            ps.setInt(1, height);
            ps.setInt(2, cid);
            numCount = ps.executeUpdate();

            if (numCount != 1) {
                return false;
            }

        } catch (SQLException ex) {
            return false;
        }

        return true;
    }

    /**
     * Create a table containing all the countries which have a population over 100
     * million. The name of the table should be mostPopulousCountries and the
     * attributes should be:
     *        * cid INTEGER (country id)
     *        * cname VARCHAR(20) (country name)
     * Returns true if the database was successfully updated, false otherwise. Store the
     * results in ASC order according to the country id (cid).
     */
     public boolean updateDB() {
        int countBefore = 0;
        int countAfter;

        try {
	    // Query to see how many rows will be inserted into the new table.
	    // This is meant for error checking.
            ps = connection.prepareStatement("SELECT cid, cname FROM a2.country WHERE population > 100000000 ORDER BY " +
                                             "cid ASC;");
            rs = ps.executeQuery();
	    
	    // Count the number of rows.
            while (rs.next()) {
                countBefore++;
            }

	    // Create the table.
            ps = connection.prepareStatement("CREATE TABLE a2.mostPopulousCountries (cid INTEGER, cname VARCHAR(20));");
            ps.executeUpdate();

	    // Insert into the table.
	    // NOTE: This is a trivial clarification, but 'over 100 million' does not include 100 million.
            ps = connection.prepareStatement("INSERT INTO a2.mostPopulousCountries (SELECT cid, cname FROM a2.country " +
                                             "WHERE population > 100000000 ORDER BY cid ASC);");

            countAfter = ps.executeUpdate();
	    
	    // Check to make sure that the correct number of rows were inserted.
            if (countAfter != countBefore) {
                return false;
            }

        } catch (SQLException ex) {
            return false;
        }

        return true;
    }
}
