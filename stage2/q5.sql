SELECT c.country_name FROM countries c JOIN regions r ON c.region_id=r.region_id WHERE r.region_name="Europe";
