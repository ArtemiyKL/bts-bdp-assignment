from typing import Annotated
from fastapi import APIRouter, status, HTTPException, Query
import psycopg2
from psycopg2.extras import RealDictCursor
from bdi_api.settings import Settings

settings = Settings()

s5 = APIRouter(
    responses={
        status.HTTP_404_NOT_FOUND: {"description": "Not found"},
        status.HTTP_422_UNPROCESSABLE_ENTITY: {"description": "Something is wrong with the request"},
    },
    prefix="/api/s5",
    tags=["Exercise 5"],
)

def get_db_connection():
    try:
        # Connect to the database defined in BDI_DB_URL
        conn = psycopg2.connect(settings.db_url, cursor_factory=RealDictCursor)
        conn.autocommit = True
        return conn
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database connection error: {e}")

@s5.post("/db/init")
def init_database() -> str:
    """Create all HR tables (run schema SQL)"""
    schema_sql = """
    DROP TABLE IF EXISTS salary_history CASCADE;
    DROP TABLE IF EXISTS employee_project CASCADE;
    DROP TABLE IF EXISTS project CASCADE;
    DROP TABLE IF EXISTS employee CASCADE;
    DROP TABLE IF EXISTS department CASCADE;

    CREATE TABLE department (
        id SERIAL PRIMARY KEY,
        name VARCHAR(50) NOT NULL,
        location VARCHAR(50) NOT NULL
    );
    CREATE TABLE employee (
        id SERIAL PRIMARY KEY,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NOT NULL,
        email VARCHAR(100) UNIQUE NOT NULL,
        hire_date DATE NOT NULL,
        salary NUMERIC(10, 2) NOT NULL,
        department_id INTEGER REFERENCES department(id)
    );
    CREATE TABLE project (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100) NOT NULL,
        budget NUMERIC(12, 2) NOT NULL
    );
    CREATE TABLE employee_project (
        employee_id INTEGER REFERENCES employee(id),
        project_id INTEGER REFERENCES project(id),
        PRIMARY KEY (employee_id, project_id)
    );
    CREATE TABLE salary_history (
        id SERIAL PRIMARY KEY,
        employee_id INTEGER REFERENCES employee(id),
        change_date DATE NOT NULL,
        old_salary NUMERIC(10, 2),
        new_salary NUMERIC(10, 2),
        reason VARCHAR(200)
    );
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(schema_sql)
    return "OK"

@s5.post("/db/seed")
def seed_database() -> str:
    """Populate tables with sample data"""
    seed_sql = """
    INSERT INTO department (name, location) VALUES
    ('HR', 'New York'), ('IT', 'San Francisco'), ('Sales', 'Chicago'), ('Marketing', 'New York'), ('Finance', 'London');

    INSERT INTO employee (first_name, last_name, email, hire_date, salary, department_id) VALUES
    ('John', 'Doe', 'john.doe@example.com', '2020-01-15', 75000.00, 2),
    ('Jane', 'Smith', 'jane.smith@example.com', '2019-05-20', 85000.00, 2),
    ('Michael', 'Brown', 'michael.brown@example.com', '2021-03-10', 60000.00, 1),
    ('Emily', 'Davis', 'emily.davis@example.com', '2018-11-05', 95000.00, 3),
    ('David', 'Wilson', 'david.wilson@example.com', '2022-07-01', 55000.00, 1),
    ('Sarah', 'Miller', 'sarah.miller@example.com', '2020-09-15', 72000.00, 4),
    ('Robert', 'Taylor', 'robert.taylor@example.com', '2017-02-28', 105000.00, 5),
    ('Jennifer', 'Anderson', 'jennifer.anderson@example.com', '2021-12-01', 68000.00, 4),
    ('William', 'Thomas', 'william.thomas@example.com', '2019-08-10', 82000.00, 2),
    ('Jessica', 'Jackson', 'jessica.jackson@example.com', '2023-01-20', 58000.00, 3),
    ('Christopher', 'White', 'christopher.white@example.com', '2016-06-15', 110000.00, 5),
    ('Amanda', 'Harris', 'amanda.harris@example.com', '2022-04-05', 62000.00, 1);

    INSERT INTO project (name, budget) VALUES
    ('Website Redesign', 50000.00), ('Mobile App Development', 100000.00), ('Q4 Marketing Campaign', 75000.00),
    ('Employee Onboarding System', 30000.00), ('Financial Audit', 25000.00), ('Customer Loyalty Program', 60000.00);

    INSERT INTO employee_project (employee_id, project_id) VALUES
    (1, 1), (1, 2), (2, 2), (2, 4), (3, 4), (4, 3), (4, 6), (5, 4), (6, 3), (7, 5), (8, 3), (9, 1), (9, 2), (10, 6), (11, 5), (12, 4);

    INSERT INTO salary_history (employee_id, change_date, old_salary, new_salary, reason) VALUES
    (1, '2021-03-15', 70000.00, 75000.00, 'Annual review'),
    (2, '2020-05-20', 80000.00, 85000.00, 'Promotion'),
    (4, '2020-11-05', 90000.00, 95000.00, 'Market adjustment'),
    (7, '2019-02-28', 95000.00, 100000.00, 'Annual review'),
    (7, '2021-02-28', 100000.00, 105000.00, 'Seniority increase'),
    (11, '2018-06-15', 100000.00, 105000.00, 'Performance bonus'),
    (11, '2021-06-15', 105000.00, 110000.00, 'Annual review');
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(seed_sql)
    return "OK"

@s5.get("/departments/")
def list_departments() -> list[dict]:
    """List all departments"""
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT * FROM department")
            return cur.fetchall()

@s5.get("/employees/")
def list_employees(
    page: Annotated[int, Query(ge=1)] = 1,
    per_page: Annotated[int, Query(ge=1, le=100)] = 10,
) -> list[dict]:
    """List employees with pagination"""
    offset = (page - 1) * per_page
    query = """
        SELECT e.id, e.first_name, e.last_name, e.email, e.salary, d.name as department_name
        FROM employee e
        JOIN department d ON e.department_id = d.id
        ORDER BY e.id
        LIMIT %s OFFSET %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (per_page, offset))
            return cur.fetchall()

@s5.get("/departments/{dept_id}/employees")
def list_department_employees(dept_id: int) -> list[dict]:
    """Employees in a specific department"""
    query = """
        SELECT e.id, e.first_name, e.last_name, e.email, e.salary, e.hire_date
        FROM employee e
        WHERE e.department_id = %s
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (dept_id,))
            return cur.fetchall()

@s5.get("/departments/{dept_id}/stats")
def department_stats(dept_id: int) -> dict:
    """Department KPIs"""
    query = """
        SELECT 
            d.name as department_name,
            COUNT(DISTINCT e.id) as employee_count,
            AVG(e.salary) as avg_salary,
            COUNT(DISTINCT ep.project_id) as project_count
        FROM department d
        LEFT JOIN employee e ON d.id = e.department_id
        LEFT JOIN employee_project ep ON e.id = ep.employee_id
        WHERE d.id = %s
        GROUP BY d.name
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (dept_id,))
            result = cur.fetchone()
            if not result:
                raise HTTPException(status_code=404, detail="Department not found")
            return result

@s5.get("/employees/{emp_id}/salary-history")
def salary_history(emp_id: int) -> list[dict]:
    """Salary evolution for an employee"""
    query = """
        SELECT change_date, old_salary, new_salary, reason
        FROM salary_history
        WHERE employee_id = %s
        ORDER BY change_date
    """
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (emp_id,))
            return cur.fetchall()