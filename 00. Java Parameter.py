# kena run with parameter
# python testPython.py 1 "New Document Name" "Job1" "Completed" 123 "user@example.com"

import os
import sys
import pyodbc
 
print("Arguments passed:", sys.argv)
 
# Database connection setup
def connect_to_mssql():
    try:
        connection = pyodbc.connect(
            'DRIVER={ODBC Driver 17 for SQL Server};'
            'SERVER=10.32.1.51,1455;'
            'DATABASE=mis_db_prod_backup_2024_04_02;'
            'UID=mis_admin;'
            'PWD=Exim1234;'
            'Encrypt=yes;TrustServerCertificate=yes'  # Use if you encounter SSL issues
        )
        print("Connected to MSSQL database successfully.")
        return connection
    except Exception as e:
        print(f"Error connecting to MSSQL database: {e}")
        sys.exit(1)
 
# Function to update user data
def set_user(connection, documentId, documentName, jobName, statusName, uploadedById, uploadedByEmail, reportingDate):
    print("Starting user update...")
    try:
        # Open a cursor to interact with the database
        with connection.cursor() as cursor:
            # Update the user data in the 'users' table
            cursor.execute(
                "UPDATE users SET username = ? WHERE userId = ?",
                ('rozaimizamahriMISPYTHON', 1)
            )
            # Commit the changes
            connection.commit()
 
        print("User updated successfully.")
    except Exception as e:
        print(f"Error updating user: {e}")
 
# Main function
if __name__ == "__main__":
    try:
        # Ensure we have the correct number of arguments
        if len(sys.argv) != 8:
            print("Usage: python testPython.py <documentId> <documentName> <jobName> <statusName> <uploadedById> <uploadedByEmail> <reportingDate>")
            sys.exit(1)
 
        # Parse command-line arguments
        documentId = int(sys.argv[1])
        documentName = sys.argv[2]
        jobName = sys.argv[3]
        statusName = sys.argv[4]
        uploadedById = int(sys.argv[5])
        uploadedByEmail = sys.argv[6]
        reportingDate = sys.argv[7] # YYYY-MM-DD
 
        print(f"Arguments received: {documentId}, {documentName}, {jobName}, {statusName}, {uploadedById}, {uploadedByEmail}, {reportingDate}")
 
        # Connect to MSSQL
        connection = connect_to_mssql()
 
        # Call the set_user function with the parsed arguments
        set_user(connection, documentId, documentName, jobName, statusName, uploadedById, uploadedByEmail, reportingDate)
 
    except Exception as e:
        print(f"Script failed with exception: {e}")
        sys.exit(1)  # Exit the script with a failure code
    finally:
        if 'connection' in locals() and connection is not None:
            connection.close()
            print("Database connection closed.")
 
 