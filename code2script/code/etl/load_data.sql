INSERT INTO target_table
SELECT 
    id,
    name,
    email
FROM source_table
WHERE date = '${bizdate}';
