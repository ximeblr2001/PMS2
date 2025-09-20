import streamlit as st
import psycopg2
import pandas as pd
import plotly.express as px
from datetime import datetime

# --- 1. Database Connection & Setup ---
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

# --- 2. Data Access & CRUD Operations ---
def run_query(query, params=None, fetch=True):
    """A general-purpose method to run SQL queries and handle data."""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame() if fetch else False
    
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        if fetch:
            cols = [desc[0] for desc in cursor.description]
            data = cursor.fetchall()
            return pd.DataFrame(data, columns=cols)
        else:
            conn.commit()
            return True
    except Exception as e:
        conn.rollback()
        st.error(f"Database operation failed: {e}")
        return False
    finally:
        cursor.close()

def get_goals(employee_id=None):
    query = "SELECT g.goal_id, g.description, g.due_date, g.status, e.name as employee_name FROM goal g JOIN employee e ON g.employee_id = e.employee_id"
    if employee_id:
        query += " WHERE g.employee_id = %s"
        return run_query(query, (employee_id,))
    return run_query(query)

def get_tasks(goal_id):
    query = "SELECT task_id, description, is_approved FROM task WHERE goal_id = %s;"
    return run_query(query, (goal_id,))

def get_feedback(employee_id):
    query = """
        SELECT f.content, f.timestamp, g.description as goal_description
        FROM feedback f
        JOIN goal g ON f.goal_id = g.goal_id
        WHERE g.employee_id = %s;
    """
    return run_query(query, (employee_id,))

def create_goal(employee_id, manager_id, description, due_date, status='In Progress'):
    query = "INSERT INTO goal (employee_id, manager_id, description, due_date, status) VALUES (%s, %s, %s, %s, %s);"
    return run_query(query, (employee_id, manager_id, description, due_date, status), fetch=False)

def create_task(goal_id, description):
    query = "INSERT INTO task (goal_id, description) VALUES (%s, %s);"
    return run_query(query, (goal_id, description), fetch=False)

def create_feedback(goal_id, manager_id, content):
    query = "INSERT INTO feedback (goal_id, manager_id, content) VALUES (%s, %s, %s);"
    return run_query(query, (goal_id, manager_id, content), fetch=False)

def update_goal_status(goal_id, new_status):
    query = "UPDATE goal SET status = %s WHERE goal_id = %s;"
    return run_query(query, (new_status, goal_id), fetch=False)

def approve_task(task_id):
    query = "UPDATE task SET is_approved = TRUE WHERE task_id = %s;"
    return run_query(query, (task_id,), fetch=False)

def delete_goal(goal_id):
    query = "DELETE FROM goal WHERE goal_id = %s;"
    return run_query(query, (goal_id,), fetch=False)

def get_business_insights():
    """Gathers key business metrics for the dashboard."""
    insights = {}
    
    # Corrected query to handle cases with no goals
    query1 = "SELECT status, COUNT(*) as total_goals FROM goal GROUP BY status;"
    insights['goal_status'] = run_query(query1)
    
    # Fix for the TypeError: Using COALESCE to return 0 instead of NULL
    query2 = "SELECT COALESCE(AVG(goal_count), 0) FROM (SELECT COUNT(*) AS goal_count FROM goal GROUP BY employee_id) as subquery;"
    result = run_query(query2)
    insights['avg_goals_per_employee'] = result.iloc[0, 0] if not result.empty else 0
    
    query3 = """
        SELECT e.name, COUNT(t.task_id) as total_tasks
        FROM task t JOIN goal g ON t.goal_id = g.goal_id
        JOIN employee e ON g.employee_id = e.employee_id
        GROUP BY e.name ORDER BY total_tasks DESC LIMIT 1;
    """
    df_tasks = run_query(query3)
    insights['most_productive_employee'] = df_tasks.iloc[0, 0] if not df_tasks.empty else "N/A"
    
    query4 = """
        SELECT e.name, COUNT(f.feedback_id) as total_feedback
        FROM feedback f JOIN goal g ON f.goal_id = g.goal_id
        JOIN employee e ON g.employee_id = e.employee_id
        GROUP BY e.name ORDER BY total_feedback DESC LIMIT 1;
    """
    df_feedback = run_query(query4)
    insights['most_feedback_employee'] = df_feedback.iloc[0, 0] if not df_feedback.empty else "N/A"
    
    return insights

# --- 3. Streamlit UI (Frontend) ---
st.set_page_config(layout="wide")
st.title("🎯 Performance Management System")

# Navigation
st.sidebar.title("Navigation")
page = st.sidebar.radio("Go to", ["Dashboard", "Goal Management", "Task Management", "Feedback & History"])

# --- Dashboard ---
if page == "Dashboard":
    st.header("Analytics & Business Insights")
    insights = get_business_insights()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Avg Goals per Employee", f"{insights['avg_goals_per_employee']:.2f}")
    with col2:
        st.metric("Most Productive Employee", insights['most_productive_employee'])
    with col3:
        st.metric("Employee with Most Feedback", insights['most_feedback_employee'])
    with col4:
        st.metric("Total Goals", insights['goal_status']['total_goals'].sum() if not insights['goal_status'].empty else 0)

    st.markdown("---")
    
    st.subheader("Goal Status Breakdown")
    if not insights['goal_status'].empty:
        fig_pie = px.pie(insights['goal_status'], values='total_goals', names='status', title='Goal Status Distribution')
        st.plotly_chart(fig_pie, use_container_width=True)
    else:
        st.info("No data available for goals.")

# --- Goal Management (CRUD) ---
elif page == "Goal Management":
    st.header("Goal Management (Manager)")
    
    st.subheader("Create a New Goal")
    with st.form("create_goal_form"):
        employee_id = st.number_input("Employee ID", min_value=1)
        manager_id = st.number_input("Your Manager ID", min_value=1)
        description = st.text_area("Goal Description")
        due_date = st.date_input("Due Date")
        submitted = st.form_submit_button("Set Goal")
        if submitted:
            if create_goal(employee_id, manager_id, description, due_date):
                st.success("Goal set successfully!")
            else:
                st.error("Failed to set goal.")

    st.markdown("---")
    
    st.subheader("View & Manage Goals")
    df_goals = get_goals()
    if not df_goals.empty:
        st.dataframe(df_goals, use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Update Goal Status")
            goal_id_to_update = st.number_input("Goal ID to update status", min_value=1)
            new_status = st.selectbox("New Status", ['In Progress', 'Completed', 'Cancelled'])
            if st.button("Update Status"):
                if update_goal_status(goal_id_to_update, new_status):
                    st.success("Goal status updated!")
                    st.experimental_rerun()
                else:
                    st.error("Failed to update goal status.")
        
        with col2:
            st.subheader("Delete a Goal")
            goal_id_to_delete = st.number_input("Goal ID to delete", min_value=1)
            if st.button("Delete Goal"):
                if delete_goal(goal_id_to_delete):
                    st.success("Goal deleted successfully.")
                    st.experimental_rerun()
                else:
                    st.error("Failed to delete goal.")
    else:
        st.info("No goals have been set yet.")

# --- Task Management ---
elif page == "Task Management":
    st.header("Task Management (Employee)")
    
    st.subheader("Log a New Task")
    with st.form("log_task_form"):
        goal_id = st.number_input("Goal ID", min_value=1)
        task_description = st.text_area("Task Description")
        submitted = st.form_submit_button("Log Task")
        if submitted:
            if create_task(goal_id, task_description):
                st.success("Task logged successfully!")
            else:
                st.error("Failed to log task.")
    
    st.markdown("---")
    
    st.subheader("Approve Tasks (Manager)")
    df_goals_with_tasks = get_goals()
    if not df_goals_with_tasks.empty:
        goal_id_to_view = st.selectbox(
            "Select Goal to View Tasks", 
            df_goals_with_tasks['goal_id'], 
            format_func=lambda x: f"Goal {x}: {df_goals_with_tasks[df_goals_with_tasks['goal_id'] == x]['description'].iloc[0]}"
        )
        df_tasks = get_tasks(goal_id_to_view)
        
        if not df_tasks.empty:
            st.dataframe(df_tasks, use_container_width=True)
            task_id_to_approve = st.number_input("Task ID to Approve", min_value=1)
            if st.button("Approve Task"):
                if approve_task(task_id_to_approve):
                    st.success("Task approved!")
                    st.experimental_rerun()
                else:
                    st.error("Failed to approve task.")
        else:
            st.info("No tasks logged for this goal.")
    else:
        st.info("No goals to manage tasks for.")

# --- Feedback & Reporting ---
elif page == "Feedback & History":
    st.header("Feedback & Performance History")
    
    st.subheader("Provide Feedback (Manager)")
    with st.form("feedback_form"):
        goal_id = st.number_input("Goal ID", min_value=1)
        manager_id = st.number_input("Your Manager ID", min_value=1)
        content = st.text_area("Feedback Content")
        submitted = st.form_submit_button("Submit Feedback")
        if submitted:
            if create_feedback(goal_id, manager_id, content):
                st.success("Feedback submitted successfully!")
            else:
                st.error("Failed to submit feedback.")
    
    st.markdown("---")
    
    st.subheader("Performance History (Employee)")
    employee_id_history = st.number_input("Enter Employee ID to view history", min_value=1)
    
    st.markdown("#### Goal History")
    df_goals_history = get_goals(employee_id=employee_id_history)
    if not df_goals_history.empty:
        st.dataframe(df_goals_history, use_container_width=True)
    else:
        st.info("No goals found for this employee.")
    
    st.markdown("#### Feedback History")
    df_feedback_history = get_feedback(employee_id=employee_id_history)
    if not df_feedback_history.empty:
        st.dataframe(df_feedback_history, use_container_width=True)
    else:
        st.info("No feedback found for this employee.")
