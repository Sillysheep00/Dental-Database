import mysql.connector
import csv
import plotly.graph_objects as go 
import plotly.io as pio

pio.renderers.default = "browser"
def create_mysql_connection(host_name, user_name, user_password):
    """
    Creates and returns a MySQL connection.
    """
    try:
        connection = mysql.connector.connect(
            host='',
            user='',
            password=''
        )
        if connection.is_connected():
            print("Connected to MySQL")
            return connection
    except Exception as e:
        print(f"Error: '{e}'")
        return None


def create_database(connection, db_name):
    """
    Creates a database if it doesn't already exist.
    """
    try:
        cursor = connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_name}")
        print(f"Database '{db_name}' created.")
        cursor.close()
    except Exception as e:
        print(f"Error: '{e}'")

def create_table(connection,db_name):
    """
    Creates 'Dentist','Patient','Appointment','Billing' tables with specified
    attributes and relationship
    """

    try:
        connection.database = db_name
        cursor = connection.cursor()

        #Create Dentist table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Dentist (
            dentist_id INT PRIMARY KEY,
            name VARCHAR(100),
            specialty VARCHAR(100),
            phone_number VARCHAR(15),
            UNIQUE (phone_number)
        );
        """)
    
        #Create Patient table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Patient (
            patient_id INT PRIMARY KEY,
            name VARCHAR(100),
            gender VARCHAR(20),
            phone_number VARCHAR(15),
            UNIQUE (phone_number)       
        );
        """)

        #Create Appoointment table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Appointment (
            appointment_id INT PRIMARY KEY,
            appointment_date DATE NOT NULL,
            appointment_time TIME NOT NULL,
            patient_id INT,
            dentist_id INT,
            FOREIGN KEY (patient_id) REFERENCES Patient(patient_id),
            FOREIGN KEY (dentist_id) REFERENCES Dentist(dentist_id),
            UNIQUE (patient_id, dentist_id, appointment_date, appointment_time)
        );
        """)
        
        #Create Billing table
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS Billing (
            billing_id INT PRIMARY KEY,
            appointment_id INT NOT NULL,
            amount DECIMAL(10, 2) NOT NULL,
            payment_status VARCHAR(50),
            FOREIGN KEY (appointment_id) REFERENCES Appointment(appointment_id) ON DELETE CASCADE,
            UNIQUE (appointment_id)
        );
        """)

        print("Tables 'Dentist','Patient','Appointment','Billing' created.")
        cursor.close()
    except Exception as e:
        print(f"Error:{e}")

def populate_table_from_csv(connection,csv_file_path,table_name):
    """
    Populates a specified table from a CSV file
    """

    try:
        cursor = connection.cursor()
        with open(csv_file_path,mode ='r') as file:
            csv_reader = csv.reader(file)
            next(csv_reader) # Skip header row

            # Define insert query based on table 
            if table_name == 'Dentist':
                insert_query = """
                INSERT INTO Dentist(dentist_id,name,specialty,phone_number)
                VALUES(%s,%s,%s,%s)
                """
            elif table_name == 'Patient':
                insert_query = """
                INSERT INTO Patient(patient_id,name,gender,phone_number)
                VALUES(%s,%s,%s,%s)
                """
            
            elif table_name == 'Appointment':
                insert_query = """
                INSERT INTO Appointment(appointment_id,appointment_date,appointment_time,patient_id,dentist_id)
                VALUES(%s,%s,%s,%s,%s)
                """ 
            
            elif table_name == 'Billing':
                insert_query = """
                INSERT INTO Billing(billing_id,appointment_id,amount,payment_status)
                VALUES(%s,%s,%s,%s)
                """

            #Insert each row from CSV into the table
            for row in csv_reader:
                cursor.execute(insert_query,row)
            
        connection.commit()
        print(F"Data from '{csv_file_path}' inserted successfully into '{table_name}' table.")
        cursor.close()
    except Exception as e:
        print(f"Error:'{table_name}' '{e}' ")
    

def print_all_dentist(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
        SELECT * 
        FROM Dentist
        """)
        rows = cursor.fetchall()
        print("Get all data in Dentist table")
        for row in rows:
            print(row)
        cursor.close()
    except Exception as e:
        print(f"Error executing query {e}")

def print_num_appointment(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
        SELECT COUNT(*) AS total_appointments
        FROM Appointment
        WHERE patient_id = 1021
        """)
        
        rows = cursor.fetchall()
        print("Count the number of appointments for patient_id =1021")
        for row in rows:
            print(row[0])
        cursor.close()
    except Exception as e:
        print(f"Error executing query {e}")

def print_total_billing(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
        SELECT SUM(amount) as total_billed
        FROM Billing
        WHERE appointment_id IN( 
        SELECT appointment_id
        FROM Appointment
        WHERE patient_id = 1021
        );
        """)
        rows = cursor.fetchall()
        print("Find Total Billing amount for patient 1021.")
        for row in rows:
            print(row[0])
        cursor.close()
    except Exception as e:
        print(f"Error executing query {e}")
def print_total_billing_each_patient(connection):
    try:
        cursor = connection.cursor()
        cursor.execute("""
        SELECT A.patient_id,
            SUM((SELECT B.amount
            FROM Billing B
            WHERE B.appointment_id = A.appointment_id)) as TotalBilled
        FROM Appointment A
        GROUP BY A.patient_id
        ORDER BY TotalBilled DESC;

        """)
        rows = cursor.fetchall()
        print("Find Total Billing amount for every patient.")
        for row in rows:
            print(f"{row[0]}: {row[1]}")     
        cursor.close()
    except Exception as e:
        print(f"Error executing query {e}")
def query_and_plot(connection):
    """Plot a bar chart of the total billing of each patient(patient can know how much they had spent in this clinic)"""
    try:
        cursor = connection.cursor()
        cursor.execute("""
        SELECT A.patient_id,
            SUM((SELECT B.amount
            FROM Billing B
            WHERE B.appointment_id = A.appointment_id)) as TotalBilled
        FROM Appointment A
        GROUP BY A.patient_id
        ORDER BY TotalBilled DESC;
    
        """)
        data = cursor.fetchall()
        patient_ids = [row[0] for row in data]
        total_billed = [row[1] for row in data]

        #Create a bar chart using Plotly
        bar_chart = go.Figure(data=[
            go.Bar(
                x = patient_ids,
                y = total_billed,
                text = total_billed,
                textposition ='auto',
                marker =dict(color ='royalblue')
            )
        ])

        bar_chart.update_layout(
            title = "Total Billing for each Patient",
            xaxis_title = "Patient ID",
            yaxis_title = "Total Billed Amount",
            template = "plotly_dark"
            )
        
        bar_chart.show()
    except Exception as e:
        print(f"Error executing query {e}")

def main():
    #MySQl connection details
    host_name = ""
    user_name = ""
    user_password = ""

    #Database name
    db_name = "DentalClinic"

    #File paths for CSVs
    dentist_csv_path = "dentist.csv"
    patient_csv_path = "patient.csv"
    appointment_csv_path ="appointment.csv"
    billing_csv_path ="billing.csv"

    #Create MySQL connection
    connection = create_mysql_connection(host_name,user_name,user_password)

    if connection:
        try:
            #Create the database
            create_database(connection,db_name)

            #Create the tables in the database
            create_table(connection,db_name)

            #Populate tables with data from the CSV files
    
            populate_table_from_csv(connection,dentist_csv_path,"Dentist")
            populate_table_from_csv(connection,patient_csv_path,"Patient")
            populate_table_from_csv(connection,appointment_csv_path,"Appointment")
            populate_table_from_csv(connection,billing_csv_path,"Billing")

            #Executes queries and print results
            print_all_dentist(connection)
            print_num_appointment(connection)
            print_total_billing(connection)
            print_total_billing_each_patient(connection)

            #Query and plot bar chart
            query_and_plot(connection)

           
        finally:
            # Close the MySQL connection
            connection.close()
            print("MySQL connection closed.")
    else:
        print("Failed to connect to MySQL")

# Run the main function
if __name__ == "__main__":
    main()