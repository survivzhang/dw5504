SELECT 
    Gender, 
    Time_of_Day, 
    State, 
    National_Road_Type,  
    COUNT(Crash_ID) AS accident_Count
FROM 
    Accident_Facts AF
JOIN 
    Time_Dimension TD ON AF.TimeID = TD.TimeID
JOIN 
    Location_Dimension LD ON AF.LocationID = LD.LocationID
JOIN 
    Personnel_Dimension PD ON AF.PersonnelID = PD.PersonnelID
JOIN 
    Road_Type_Dimension RTD ON AF.RoadID = RTD.RoadID
WHERE 
    National_Road_Type = 'Local Road' 
    AND Gender = 'Male'
    AND Gender IS NOT NULL
    AND Time_of_Day IS NOT NULL
    AND State IS NOT NULL
    AND National_Road_Type IS NOT NULL
GROUP BY 
    CUBE(Gender, Time_of_Day, State, National_Road_Type);
