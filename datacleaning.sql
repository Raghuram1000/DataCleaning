-- Create a staging table with the same structure as the 'layoffs' table
CREATE TABLE layoffstaging
LIKE layoffs;

-- Insert all data from 'layoffs' into 'layoffstaging'
INSERT INTO layoffstaging
SELECT *
FROM layoffs;

-- Identify duplicate rows using ROW_NUMBER and Common Table Expression (CTE)
WITH duplicate_cte AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY 
                   company, 
                   location, 
                   industry, 
                   total_laid_off, 
                   percentage_laid_off, 
                   date, 
                   stage, 
                   country, 
                   funds_raised_millions
           ) AS row_num
    FROM layoffstaging
)
-- Select duplicate rows (row_num > 1)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;

CREATE TABLE `layoffstaging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;
SELECT *
FROM layoffstaging2;
INSERT INTO layoffstaging2
SELECT *,
 ROW_NUMBER() OVER (
               PARTITION BY 
                   company, 
                   location, 
                   industry, 
                   total_laid_off, 
                   percentage_laid_off, 
                   date, 
                   stage, 
                   country, 
                   funds_raised_millions
           )
FROM layoffstaging;
SET SQL_SAFE_UPDATES = 0;
DELETE
FROM layoffstaging2
WHERE row_num > 1;
SELECT *
FROM layoffstaging2;
-- Standardizing the data
SELECT company,TRIM(company)
FROM layoffstaging2;
UPDATE layoffstaging2
SET company=TRIM(company);
SELECT distinct industry 
FROM layoffstaging2
ORDER BY 1;
SELECT *
FROM layoffstaging2
WHERE industry LIKE 'Crypto %';
UPDATE layoffstaging2
SET industry='Crypto'
WHERE industry LIKE 'Crypto %';
SELECT DISTINCT country,TRIM(Trailing '.' From country)
FROM layoffstaging2
Order By 1;
Update layoffstaging2
SET country=TRIM(Trailing '.' From country)
Where country LIKE ' United State %';
SELECT *
FROM layoffstaging2;
SELECT 
    `date`,
    STR_TO_DATE(`date`, '%m/%d/%y') AS formatted_date
FROM 
    layoffstaging2;
UPDATE layoffstaging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%y');
UPDATE layoffstaging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

ALTER Table layoffstaging2
Modify column `date` DATE;
SELECT *
FROM layoffstaging2 AS t1
JOIN layoffstaging2 AS t2
ON t1.company = t2.company
   AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = '') 
  AND t2.industry IS NOT NULL;
Update layoffstaging2
SET industry=NULL
Where industry='';
UPDATE layoffstaging2 t1
JOIN layoffstaging2 t2
  ON t1.company = t2.company
 AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE (t1.industry IS NULL OR t1.industry = '') 
  AND t2.industry IS NOT NULL;
SELECT *
FROM layoffstaging2;
DELETE
FROM layoffstaging2
Where total_laid_off is NULL and percentage_laid_off is NULL;

SELECT *
FROM layoffstaging2;

ALTER Table layoffstaging2
DROP column row_num;
SELECT *
FROM layoffstaging2;

SELECT max(total_laid_off),max(percentage_laid_off)
FROM layoffstaging2;
SELECT *
FROM layoffstaging2
WHERE percentage_laid_off=1
ORDER BY funds_raised_millions DESC;
SELECT company,SUM(total_laid_off)
FROM layoffstaging2
group by company
Order by 2 DESC;
SELECT industry,SUM(total_laid_off)
FROM layoffstaging2
group by industry
Order by 2 DESC;
SELECT country,SUM(total_laid_off)
FROM layoffstaging2
group by country
Order by 2 DESC;
SELECT YEAR(`date`),SUM(total_laid_off)
FROM layoffstaging2
group by year(`date`)
Order by 1 Desc;
SELECT stage,SUM(total_laid_off)
FROM layoffstaging2
group by stage
Order by 1 Desc;
Select substring(`date`,1,7) AS `Month`,sum(total_laid_off) AS totaloff
From layoffstaging2
group by `Month`
Order By 1 Asc;
With Rolling_Total AS
(Select substring(`date`,1,7) AS `Month`,sum(total_laid_off) AS totaloff
From layoffstaging2
group by `Month`
Order By 1 Asc)
Select  `Month`,Sum(totaloff) Over(Order By  `Month`)
From Rolling_Total;
