SELECT d.department_name FROM departments d JOIN employees e ON e.department_id=d.department_id ORDER BY e.salary DESC LIMIT 1;
