DELETE FROM `311_complaints_analysis.best_cities_with_least_complaints` WHERE 1=1;
INSERT INTO `311_complaints_analysis.best_cities_with_least_complaints`
SELECT LOWER(City),COUNT(Unique_key),Year
FROM `311_complaints_analysis.raw_data`
WHERE LOWER(CITY) IS NOT NULL
AND LOWER(CITY) NOT IN ('laguardia airport')
GROUP BY LOWER(City), Year;

DELETE FROM `311_complaints_analysis.status_chart` WHERE 1=1;
INSERT INTO `311_complaints_analysis.status_chart`
SELECT Year,Status,COUNT(*) AS Count_val from `311_complaints_analysis.raw_data`
GROUP BY Year,Status;

DELETE FROM `311_complaints_analysis.City_Comaplaints` WHERE 1=1;
INSERT INTO `311_complaints_analysis.City_Comaplaints`
SELECT LOWER(City),LOWER(Complaint_Type),COUNT(*) AS City_Complaints from `311_complaints_analysis.raw_data`
WHERE LOWER(City) is NOT NULL
GROUP BY LOWER(City),LOWER(Complaint_Type);

DELETE FROM `311_complaints_analysis.city_incident_zip`  WHERE 1=1;
INSERT  INTO `311_complaints_analysis.city_incident_zip`
SELECT LOWER(City),Incident_zip,LOWER(Complaint_Type),COUNT(Unique_key) as City_to_zip_count,Year from `311_complaints_analysis.raw_data`
WHERE LOWER(City) is not NULL
AND LOWER(Complaint_Type) is not NULL
GROUP BY LOWER(City),Incident_zip,LOWER(Complaint_Type),Year;