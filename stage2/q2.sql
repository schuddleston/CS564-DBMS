SELECT d.department_name, COUNT(*) AS employee_count FROM employees e JOIN departments d ON d.department_id=e.department_id GROUP BY d.department_name ORDER BY employee_count DESC;
