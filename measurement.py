import mysql.connector as sql
import urllib.request
import json
import time

class Measurement:
    def __init__(self, projects, host="db.int.uni-rostock.de", user="energy", password="me3Aeph1", database="energy"):
        self.projects = projects
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.db = sql.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database
        )
        
    def get_or_create_project_id(self, project_name):
        self.mycursor = self.db.cursor()

        # Check if the project_name exists in the projects table
        self.mycursor.execute("SELECT project_ID FROM projects WHERE project_name = %s", (project_name,))
        existing_project_id = self.mycursor.fetchone()

        if existing_project_id:
            # Project already exists, return the existing project_ID
            return existing_project_id[0]
        else:
            # Project doesn't exist, insert a new row and return the new project_ID
            self.mycursor.execute("INSERT INTO projects (project_name) VALUES (%s)", (project_name,))
            self.db.commit()

            # Fetch the last inserted project_ID
            self.mycursor.execute("SELECT LAST_INSERT_ID()")
            new_project_id = self.mycursor.fetchone()[0]

            return new_project_id
        
    def get_or_create_task_id(self, task_name):
        self.mycursor = self.db.cursor()

        # Check if the task_name exists in the tasks table
        self.mycursor.execute("SELECT task_ID FROM tasks WHERE task_name = %s", (task_name,))
        existing_task_id = self.mycursor.fetchone()

        if existing_task_id:
            # Task already exists, return the existing task_ID
            return existing_task_id[0]
        else:
            # Task doesn't exist, insert a new row and return the new task_ID
            self.mycursor.execute("INSERT INTO tasks (task_name) VALUES (%s)", (task_name,))
            self.db.commit()

            # Fetch the last inserted task_ID
            self.mycursor.execute("SELECT LAST_INSERT_ID()")
            new_task_id = self.mycursor.fetchone()[0]

            return new_task_id

    def start(self, task_name, url="http://139.30.207.224/netio.json"):
        self.url = url
        self.task_name = task_name
        self.contents = urllib.request.urlopen(url).read()
        start_output_1 = json.loads(self.contents)['Outputs'][0]['Energy']
        start_output_2 = json.loads(self.contents)['Outputs'][1]['Energy']
        time_start = time.time()

        self.mycursor = self.db.cursor()

        # Get or create the task_ID based on task_name
        task_id = self.get_or_create_task_id(self.task_name)

        # Get or create the project_ID based on the class attribute projects
        project_id = self.get_or_create_project_id(self.projects)

        # Insert a new row with auto-incremented measurement_ID for start measurements
        self.mycursor.execute(
            "INSERT INTO measurements (project_ID, task_ID, time_start, E0_start, E1_start) VALUES (%s, %s, %s, %s, %s)",
            (project_id, task_id, time_start, start_output_1, start_output_2)
        )

        # Fetch the last inserted measurement_ID for start measurements
        self.mycursor.execute("SELECT LAST_INSERT_ID()")
        measurement_id_start = self.mycursor.fetchone()[0]

        # Print the inserted row
        self.mycursor.execute("SELECT * FROM measurements WHERE measurement_ID = %s", (measurement_id_start,))
        for x in self.mycursor:
            print("Start Measurement:"+"E0 = {} and E1 = {}".format(start_output_1, start_output_2))
            #print(x)

        return measurement_id_start

    def stop(self, measurement_id_start):
        self.contents = urllib.request.urlopen(self.url).read()
        stop_output_1 = json.loads(self.contents)['Outputs'][0]['Energy']
        stop_output_2 = json.loads(self.contents)['Outputs'][1]['Energy']
        time_stop = time.time()

        self.mycursor = self.db.cursor()

        # Update the row with stop measurements
        self.mycursor.execute(
            "UPDATE measurements SET time_stop = %s, E0_stop = %s, E1_stop = %s WHERE measurement_ID = %s",
            (time_stop, stop_output_1, stop_output_2, measurement_id_start)
        )

        self.db.commit()

        # Print the updated row
        self.mycursor.execute("SELECT * FROM measurements WHERE measurement_ID = %s", (measurement_id_start,))
        for x in self.mycursor:
            print("Stop Measurement:"+"E0 = {} and E1 = {}".format(stop_output_1, stop_output_2))
            #print(x)
