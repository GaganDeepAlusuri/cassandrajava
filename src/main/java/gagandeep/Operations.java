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
                System.out.println("Choose any of the options:\n1. Add records\n2. Read a table\n3. Update a record in a table.\n4. Delete a Table\n5. Create a new table.\n6. Quit");
                String operation = scanner.nextLine().toUpperCase();

                if (operation.equals("6")) {
                    break;
                }

                switch (operation) {
                    case "1":
                        createRecord(session, scanner);
                        break;
                    case "2":
                        readTable(session, scanner);
                        break;
                    case "3":
                        updateBook(session, scanner);
                        break;
                    case "4":
                        deleteBook(session, scanner);
                        break;
                    case "5":
                        createTable(session, scanner);
                        break;
                }
            }

            session.close();
        } else {
            System.out.println("Error: Failed to connect to Cassandra database.");
        }
    }

    private static void createRecord(CqlSession session, Scanner scanner) {
        System.out.println("Select the table to insert records into:");
        System.out.println("1. ks_books.books");
        System.out.println("2. ks_books.fiction");
        System.out.println("3. ks_books.novels");
        int choice = scanner.nextInt();
        scanner.nextLine(); // Consume newline left-over
    
        String table;
        String columns;
        switch (choice) {
            case 1:
                table = "ks_books.books";
                columns = "title, author, price";
                break;
            case 2:
                table = "ks_books.fiction";
                columns = "author, title, year";
                break;
            case 3:
                table = "ks_books.novels";
                columns = "title, author, price";
                break;
            default:
                System.out.println("Invalid choice.");
                return;
        }
    
        System.out.println("Enter the following columns for the " + table + " table: " + columns);
        String[] colArray = columns.split(",\\s*");
        Map<String, Object> values = new HashMap<>();
        for (String column : colArray) {
            System.out.println("Enter " + column + ":");
            String input = scanner.nextLine();
            switch (column) {
                case "title":
                case "author":
                case "year":
                    values.put(column, input);
                    break;
                case "price":
                    values.put(column, Double.parseDouble(input));
                    break;
                default:
                    System.out.println("Invalid column: " + column);
                    return;
            }
        }
    
        String cql = "INSERT INTO " + table + " (" + columns + ") VALUES (";
        for (String column : colArray) {
            Object value = values.get(column);
            if (value instanceof String) {
                cql += "'" + value + "', ";
            } else if (value instanceof Double) {
                cql += value + ", ";
            }
        }
        cql = cql.substring(0, cql.length() - 2) + ")";
    
        session.execute(cql);
        System.out.println("Record added successfully.");
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
