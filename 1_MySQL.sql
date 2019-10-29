/******************************

Authors     : Vikranth Ale , Kentaro Kato
Create date : 27th March,2019
Description : USDA National Nutritional Data
Data Source : https://ndb.nal.usda.gov/ndb/
Data Source : https://courses.edx.org/courses/course-v1:MITx+15.071x+3T2018/courseware/e0d9ca1c350d42e5a8d6fd6a8162c1ab/01d6043d18d14f23a59c190490be27f9/1?activate_block_id=block-v1%3AMITx%2B15.071x%2B3T2018%2Btype%40vertical%2Bblock%4020b892d0279d4eea8d378f4d2d4a0a01

*******************************/

-- QUERY 1 ------------------------------------------------------------------------------------------------------------------------------------------------------
-- TOPIC : SELECTIONS & PROJECTIONS, EXPRESSIONS AND ORDER BY 

-- Return top 5 foods with large amout of sodium
SELECT ID,Description ,Sodium AS HighSodiumFood
FROM Food
ORDER BY Sodium DESC
LIMIT 5;

-- Return top 5 foods with large amout of Cholesterol
SELECT F.ID, Description , Cholesterol AS HighCholestralFood
FROM Food F, Fat FT
WHERE F.ID = FT.ID
ORDER BY Cholesterol DESC
LIMIT 5;


-- QUERY 2 ------------------------------------------------------------------------------------------------------------------------------------------------------
-- TOPIC: INNER JOIN

-- Return foods which are rich in proteins and vitamins based on above average consumption
SELECT F.ID,Description , P.Protein , V.VitaminC , V.VitaminD , V.VitaminE 
FROM Food F 
INNER JOIN Protein P ON F.ID = F.ID 
INNER JOIN Vitamin V ON F.ID = V.ID
WHERE P.Protein > 56 
AND V.VitaminC > 90 
AND V.VitaminD > 0.1 
AND V.VitaminE > 15
ORDER BY Protein DESC
LIMIT 10;

-- QUERY 3 ------------------------------------------------------------------------------------------------------------------------------------------------------ 
-- TOPIC : OPERATIONS ON EXPRESSIONS/ATTRIBUTES

-- Return foods with rich amount of total vitamin
SELECT  Vitamin.ID, Description, VitaminC, VitaminD, VitaminE, VitaminC+VitaminD+VitaminE AS Total_Vitamin
FROM Vitamin, Food
WHERE Food.ID = Vitamin.ID
AND VitaminC > 0
AND VitaminD > 0
AND VitaminE > 0
ORDER BY Total_Vitamin DESC;

-- QUERY 4 ------------------------------------------------------------------------------------------------------------------------------------------------------
-- TOPIC : AGGREGATE OPERATORS 

-- Return food with Average Potassium Levels
SELECT AVG(Potassium) AS AveragePotassium 
FROM  Food;

-- Return the maximum amount of carbohydrate
SELECT MAX(Carbohydrate) AS MaxCarbs
FROM Carbo;

-- Return the number of foods that protein is included more than 50
SELECT COUNT(Protein) as ProteinCount
FROM Protein
WHERE Protein > 50;

-- QUERY 5 ------------------------------------------------------------------------------------------------------------------------------------------------------
-- TOPIC : NESTED QUERY ( 3 ways < normal, IN, ANY > ) 

-- Return foods with maximum amount of iron 
SELECT Food.ID, Description, Iron
FROM Food
WHERE Iron = (SELECT MAX(Iron) FROM Food);

-- Return top 10 foods with maximum amount of calories
SELECT ID,Description ,Calories
FROM Food
WHERE Calories IN ( SELECT MAX(Calories) AS HighCalories FROM Food)
LIMIT 10;

-- Return foods that are high in both calories and proteins
SELECT DISTINCT Food.ID, Description, Calories, Protein
FROM Food, Protein
WHERE Protein.ID = Food.ID
AND Protein.ID = ANY(SELECT Food.ID
									FROM Food
									WHERE Calories > 300)
ORDER BY Protein DESC
LIMIT 10;


-- QUERY 6 ------------------------------------------------------------------------------------------------------------------------------------------------------
-- TOPIC : LIKE & BETWEEN OPERATORS 

-- Return foods that include chicken and rich in proteins with their cholesterol and enrgy levels
SELECT F.ID, Description, Protein, Calories, Cholesterol
FROM Food F, Protein P, Fat FT
WHERE F.ID = P.ID
AND F.ID  = FT.ID
AND Description LIKE 'CHICKEN%'
ORDER BY Protein DESC
LIMIT 5;

-- Return foods that include mazzarella and rich in calories, sugar and cholesterol
SELECT F.ID, F.Description, Calories, Sugar , Cholesterol 
FROM Food F 
JOIN Carbo ON F.ID = Carbo.ID
JOIN Fat ON F.ID = Fat.ID
WHERE Description LIKE '%MOZZARELLA%'
ORDER BY Cholesterol DESC
LIMIT 10;

-- Return reccommended foods to fullfill intake of average amount of calories and sugar for men and women
SELECT DISTINCT F.ID, F.Description, Calories, Sugar 
FROM Food F 
JOIN Carbo C ON F.ID = C.ID
WHERE Sugar BETWEEN 25 AND 40
ORDER BY Sugar DESC
LIMIT 10;

-- QUERY 7 ------------------------------------------------------------------------------------------------------------------------------------------------------
-- TOPIC : VIEWS

-- QUERY WITHOUT VIEW 
CREATE TABLE TotalNutrition_TABLE AS
	SELECT 	F.ID , 
					F.Description,F.Calories AS TotalEnergy,
					F.Calcium + F.Iron + F.Potassium + F.Sodium +V.VitaminC + V.VitaminD + V.VitaminE AS TotalMinerals ,
					C.Carbohydrate + C.Sugar AS TotalCarbs , FT.TotalFat , 
                    P.Protein AS TotalProtein
	FROM 	Food F 
	JOIN 		Vitamin V ON F.ID = V.ID 
	JOIN  		Carbo C ON F.ID = C.ID 
	JOIN 		Fat FT ON F.ID = FT.ID
	JOIN 		Protein P ON F.ID = P.ID;

-- VERIFYING THE ABOVE CREATED TABLE
SELECT * FROM TotalNutrition_TABLE;

-- SAME QUERY WITH VIEW 
CREATE VIEW TotalNutrition
AS
	SELECT 	F.ID , F.Description,F.Calories AS TotalEnergy, 
					F.Calcium + F.Iron + F.Potassium + F.Sodium + V.VitaminC + V.VitaminD + V.VitaminE AS TotalMinerals ,
					C.Carbohydrate + C.Sugar AS TotalCarbs , FT.TotalFat , 
                    P.Protein AS TotalProtein
	FROM Food F 
	JOIN Vitamin V ON F.ID = V.ID 
	JOIN  Carbo C ON F.ID = C.ID 
	JOIN Fat FT ON F.ID = FT.ID
	JOIN Protein P ON F.ID = P.ID;

-- VERIFYING THE ABOVE CREATED VIEW
SELECT * FROM TotalNutrition;

-- QUERY 8 ------------------------------------------------------------------------------------------------------------------------------------------------------

-- QUERY WITHOUT VIEW 
SELECT Food.ID, SUBSTRING_INDEX(Description, ',', 1) AS Main, Protein, Calories
FROM Food, Protein
WHERE Food.ID = Protein.ID;

-- QUERY WITH VIEW
CREATE VIEW MainFood
AS
	SELECT food.ID, SUBSTRING_INDEX(Description, ',', 1) AS Main, Protein, Calories
	FROM food, protein
	WHERE food.ID = protein.ID;

-- QUERY 9 ------------------------------------------------------------------------------------------------------------------------------------------------------
-- TOPIC : GROUP BY & HAVING

-- Return the group which the average amount of protein is more than 40
SELECT Main, ROUND(AVG(Protein),2) AS AVGProtein
FROM MainFood
GROUP BY Main
HAVING AVG(Protein) > 40
ORDER BY AVGProtein DESC;

            
