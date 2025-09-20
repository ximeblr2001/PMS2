import psycopg2
import pandas as pd
import streamlit as st

# --- Database Connection ---
@st.cache_resource
def get_db_connection():
    """Establishes and caches a connection to the PostgreSQL database."""
    try:
        conn = psycopg2.connect(
            dbname="pms2",
            user="postgres",
            password="Yash",
            host="localhost"
        )
        return conn
    except Exception as e:
        st.error(f"Error connecting to the database: {e}")
        return None

# --- CRUD Operations ---
class PMSBackend:
    def __init__(self):
        self.conn = get_db_connection()

    def run_query(self, query, params=None, fetch=True):
        """A general-purpose method to run SQL queries."""
        if not self.conn:
            return pd.DataFrame() if fetch else False
        
        try:
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            if fetch:
                # Get column names from the cursor description
                cols = [desc[0] for desc in cursor.description]
                data = cursor.fetchall()
                df = pd.DataFrame(data, columns=cols)
                return df
            else:
                self.conn.commit()
                return True
        except Exception as e:
            self.conn.rollback()
            st.error(f"Database operation failed: {e}")
            return False
        finally:
            cursor.close()

    # --- Create Operations ---
    def create_goal(self, employee_id, manager_id, description, due_date, status='In Progress'):
        query = "INSERT INTO goal (employee_id, manager_id, description, due_date, status) VALUES (%s, %s, %s, %s, %s);"
        return self.run_query(query, (employee_id, manager_id, description, due_date, status), fetch=False)

    def create_task(self, goal_id, description):
        query = "INSERT INTO task (goal_id, description) VALUES (%s, %s);"
        return self.run_query(query, (goal_id, description), fetch=False)

    def create_feedback(self, goal_id, manager_id, content):
        query = "INSERT INTO feedback (goal_id, manager_id, content) VALUES (%s, %s, %s);"
        return self.run_query(query, (goal_id, manager_id, content), fetch=False)

    # --- Read Operations ---
    def get_goals(self, employee_id=None):
        query = "SELECT g.goal_id, g.description, g.due_date, g.status, e.name as employee_name FROM goal g JOIN employee e ON g.employee_id = e.employee_id"
        if employee_id:
            query += " WHERE g.employee_id = %s"
            return self.run_query(query, (employee_id,))
        return self.run_query(query)

    def get_tasks(self, goal_id):
        query = "SELECT task_id, description, is_approved FROM task WHERE goal_id = %s;"
        return self.run_query(query, (goal_id,))

    def get_feedback(self, employee_id):
        query = """
            SELECT f.content, f.timestamp, g.description as goal_description
            FROM feedback f
            JOIN goal g ON f.goal_id = g.goal_id
            WHERE g.employee_id = %s;
        """
        return self.run_query(query, (employee_id,))

    # --- Update Operations ---
    def update_goal_status(self, goal_id, new_status):
        query = "UPDATE goal SET status = %s WHERE goal_id = %s;"
        return self.run_query(query, (new_status, goal_id), fetch=False)

    def approve_task(self, task_id):
        query = "UPDATE task SET is_approved = TRUE WHERE task_id = %s;"
        return self.run_query(query, (task_id,), fetch=False)

    # --- Delete Operations ---
    def delete_goal(self, goal_id):
        query = "DELETE FROM goal WHERE goal_id = %s;"
        return self.run_query(query, (goal_id,), fetch=False)

    # --- Business Insights ---
    def get_business_insights(self):
        insights = {}
        # Insight 1: Total Goals and Status Breakdown
        query1 = "SELECT status, COUNT(*) as total_goals FROM goal GROUP BY status;"
        insights['goal_status'] = self.run_query(query1)
        
        # Insight 2: Average Goals Per Employee
        query2 = "SELECT AVG(goal_count) FROM (SELECT COUNT(*) AS goal_count FROM goal GROUP BY employee_id) as subquery;"
        insights['avg_goals_per_employee'] = self.run_query(query2).iloc[0, 0] if not self.run_query(query2).empty else 0

        # Insight 3: Most Productive Employee (by tasks)
        query3 = """
            SELECT e.name, COUNT(t.task_id) as total_tasks
            FROM task t JOIN goal g ON t.goal_id = g.goal_id
            JOIN employee e ON g.employee_id = e.employee_id
            GROUP BY e.name ORDER BY total_tasks DESC LIMIT 1;
        """
        df_tasks = self.run_query(query3)
        insights['most_productive_employee'] = df_tasks.iloc[0, 0] if not df_tasks.empty else "N/A"

        # Insight 4: Employee with the most feedback
        query4 = """
            SELECT e.name, COUNT(f.feedback_id) as total_feedback
            FROM feedback f JOIN goal g ON f.goal_id = g.goal_id
            JOIN employee e ON g.employee_id = e.employee_id
            GROUP BY e.name ORDER BY total_feedback DESC LIMIT 1;
        """
        df_feedback = self.run_query(query4)
        insights['most_feedback_employee'] = df_feedback.iloc[0, 0] if not df_feedback.empty else "N/A"

        return insights
