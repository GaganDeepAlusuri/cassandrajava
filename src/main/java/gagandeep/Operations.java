package gagandeep;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinition;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.*;



import java.util.Scanner;

public class Operations {

    public static void main(String[] args) {
        CqlSession session = Connector.getSession();

        if (session != null) {
            Scanner scanner = new Scanner(System.in);

            while (true) {
                System.out.println("Enter CRUD operation (C/R/U/D) or T to create table or Q to quit:");
                String operation = scanner.nextLine().toUpperCase();

                if (operation.equals("Q")) {
                    break;
                }

                switch (operation) {
                    case "C":
                        createBook(session, scanner);
                        break;
                    case "R":
                        readTable(session, scanner);
                        break;
                    case "U":
                        updateBook(session, scanner);
                        break;
                    case "D":
                        deleteBook(session, scanner);
                        break;
                    case "T":
                        createTable(session, scanner);
                        break;
                }
            }

            session.close();
        } else {
            System.out.println("Error: Failed to connect to Cassandra database.");
        }
    }

    private static void createBook(CqlSession session, Scanner scanner) {
        System.out.println("Enter book title:");
        String title = scanner.nextLine();
        System.out.println("Enter author:");
        String author = scanner.nextLine();
        System.out.println("Enter price:");
        double price = scanner.nextDouble();

        String cql = "INSERT INTO books (title, author, price) VALUES ('" + title + "', '" + author + "', " + price + ")";
        //System.out.println(cql);
        session.execute(cql);
        System.out.println("Book added successfully.");
    }
    private static void createTable(CqlSession session, Scanner scanner) {
        
        System.out.println("Enter Table Name:");
        String TABLE_NAME = scanner.nextLine();

        System.out.print("Enter column names separated by commas: ");
        String columnNames = scanner.nextLine();
        System.out.print("Enter column types separated by commas: ");
        String columnTypes = scanner.nextLine();

        System.out.print("Enter primary key: ");
        String primaryKey = scanner.nextLine();

        System.out.print("Do you want to add clustering columns? (y/n): ");
        String addClusteringColumns = scanner.nextLine();
        String clusteringColumns = "";
        
        if (addClusteringColumns.equalsIgnoreCase("y")) {
            System.out.print("Enter clustering columns separated by commas: ");
            clusteringColumns = ", " + scanner.nextLine();
        }
        //Create a table
        String createTableQuery = "CREATE TABLE IF NOT EXISTS " + TABLE_NAME + " ("
                + getColumnDefinition(columnNames, columnTypes)
                + ", PRIMARY KEY (" + primaryKey + clusteringColumns + ")"
                + ");";
        session.execute(createTableQuery);

        System.out.println("Table Created Successfully!");
    }
    //combine column names and types
    private static String getColumnDefinition(String columnNames, String columnTypes) {
        String[] names = columnNames.split(",");
        String[] types = columnTypes.split(",");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < names.length; i++) {
            sb.append(names[i]).append(" ").append(types[i]);
            if (i < names.length - 1) {
                sb.append(", ");
            }
        }
        return sb.toString();
    }

    private static void readTable(CqlSession session, Scanner scanner) {
        System.out.println("Existing tables in the database:");
    ResultSet tablesResult = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'ks_books'");
    for (Row row : tablesResult) {
        System.out.println(row.getString("table_name"));
    }
        System.out.print("Enter the name of the table to read from: ");
        String table = scanner.nextLine();
    
        ResultSet resultSet = session.execute("SELECT table_name FROM system_schema.tables WHERE keyspace_name = 'ks_books' AND table_name = '" + table + "'");
        Row row = resultSet.one();
    
        if (row == null) {
            System.out.println("Table " + table + " does not exist in the database.");
        } else {
            resultSet = session.execute("SELECT * FROM " + table);
            List<Object> values = new ArrayList<>();
    
            for (Row r : resultSet) {
                ColumnDefinitions columnDefs = r.getColumnDefinitions();
                int numColumns = columnDefs.size();
                for (int i = 0; i < numColumns; i++) {
                    ColumnDefinition columnDef = columnDefs.get(i);
                    values.add(r.getObject(columnDef.getName()));
                }
            }
    
            if (values.isEmpty()) {
                System.out.println("No records found in table " + table);
            } else {
                int numColumns = resultSet.getColumnDefinitions().size();
                for (int i = 0; i < numColumns; i++) {
                    System.out.print(resultSet.getColumnDefinitions().get(i).getName() + "\t");
                }
                System.out.println();
    
                for (int i = 0; i < values.size(); i++) {
                    System.out.print(values.get(i) + "\t");
                    if ((i + 1) % numColumns == 0) {
                        System.out.println();
                    }
                }
            }
        }
    }
    
    
    
    
    
    

    private static void updateBook(CqlSession session, Scanner scanner) {
        System.out.println("Enter book title to update:");
        String title = scanner.nextLine();
        System.out.println("Enter new price:");
        double price = scanner.nextDouble();

        String cql = "UPDATE books SET price = " + price + " WHERE title = '" + title + "'";
        session.execute(cql);
        System.out.println("Book updated successfully.");
    }

    private static void deleteBook(CqlSession session, Scanner scanner) {
        System.out.println("Enter book title to delete:");
        String title = scanner.nextLine();

        String cql = "DELETE FROM books WHERE title = '" + title + "'";
        session.execute(cql);
        System.out.println("Book deleted successfully.");
    }
}
