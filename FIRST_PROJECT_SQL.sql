-- Data Cleaning


SELECT *
FROM layoffs ;

-- 1 Remove Duplicates
-- 2 Standardize the Data
-- 3 Null values or blank values
-- 4 Remove any columns

-- CREATING A STAGING TABLE SO AS TO KEEP THE RAW TABLE INTACT

CREATE TABLE layoffs_staging
LIKE layoffs;

SELECT * 
FROM layoffs;

INSERT INTO layoffs_staging
SELECT * 
FROM layoffs;



SELECT * 
FROM layoffs_staging;

-- Assign row numbers to detect duplicates;

SELECT *,
ROW_NUMBER() OVER( PARTITION BY 
company , location , industry , total_laid_off , percentage_laid_off , `date` , stage ,
 country ) AS Row_NUM
FROM layoffs_staging;

-- CTE to assign row numbers for detecting duplicates.

WITH Duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER( PARTITION BY 
company , location , industry , total_laid_off , percentage_laid_off , `date` , stage ,
 country , funds_raised_millions ) AS row_num
FROM layoffs_staging
)

SELECT *
FROM Duplicate_cte
WHERE Row_NUM > 1;

SELECT * 
FROM layoffs_staging
where company ='Casper';

-- Create a new version of the table with an added row_num column to handle duplicate removal.


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` int DEFAULT NULL,
  `percentage_laid_off` bigint DEFAULT NULL,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` int DEFAULT NULL,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT * 
FROM layoffs_staging2;

-- Insert data with row numbers to prepare for duplicate removal.

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER( PARTITION BY 
company , location , industry , total_laid_off , percentage_laid_off , `date` , stage ,
 country , funds_raised_millions ) AS row_num
FROM layoffs_staging
;

SELECT *
FROM layoffs_staging2
WHERE row_num >1;

-- Remove duplicates from `layoffs_staging2`, keeping only the first occurrence (row_num = 1).

DELETE
FROM layoffs_staging2
WHERE row_num >1
;



-- standardizing data

SELECT company , TRIM(company)
FROM layoffs_staging2;
-- Remove leading and trailing spaces from the values in the company column to ensure consistency.

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT * FROM layoffs_staging2;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%';

-- Standardize industry names by updating all variations starting with 'crypto' to a consistent format 'Crypto'.


UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry like 'Crypto%';


SELECT DISTINCT industry
FROM layoffs_staging2;

SELECT  DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

SELECT DISTINCT country
FROM layoffs_staging2
ORDER BY 1;

-- Clean up the country column by removing any trailing period ('.') from all rows.


UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States% '
;

-- Convert the date column from string format (MM/DD/YYYY) to proper DATE format using STR_TO_DATE 


SELECT `date`,
str_to_date(`date`,'%m/%d/%Y' )
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = str_to_date(`date`,'%m/%d/%Y' )
;

-- Change the data type of the `date` column from TEXT to DATE after converting all values to proper date format.


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs;




-- Populate the 'industry' column to eliminate NULL values by assigning appropriate default or inferred values

SELECT  DISTINCT industry 
FROM
layoffs_staging2;


UPDATE layoffs_staging2
SET industry = NULL 
WHERE industry = '';


SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL;
    
UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
    SET t1.industry = t2.industry
    WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL;



-- This query deletes rows from the 'layoffs_staging2' table where both the 'total_laid_off' and 'percentage_laid_off' columns are NULL. 
-- The intention is to remove records that do not contain any meaningful data regarding layoffs.

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL;




SELECT *
FROM layoffs_staging2;


-- This query removes the 'row_num' column from the 'layoffs_staging2' table. 
-- The column is no longer needed 

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;













