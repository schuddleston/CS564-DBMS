SELECT e.employee_id FROM employees e LEFT JOIN dependents d ON e.employee_id=d.employee_id WHERE d.employee_id IS NULL;
