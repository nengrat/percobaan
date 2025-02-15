COPY employees_ex FROM '/data/employees_ex.csv' DELIMITER AS ',' CSV HEADER;
SELECT * FROM employees_ex LIMIT 5;