-- Data Cleaning Project --

SELECT *
FROM layoffs;


-- 1. Remove duplicates
-- 2. Standarize the data
-- 3. null values or blank values 
-- 4. remove any columns 


CREATE TABLE layoffs_staging
LIKE layoffs;             # CREATE A WORK TABLE 

INSERT layoffs_staging
SELECT *                  # COPING THE DATA 
FROM layoffs;

SELECT * 
FROM layoffs_staging;


-- Identifying duplicants



WITH duplicate_cte AS 
(
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location
,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT * 
FROM duplicate_cte
WHERE row_num > 1;




CREATE TABLE `layoffs_staging2` (
  `company` text DEFAULT NULL,
  `location` text DEFAULT NULL,
  `industry` text DEFAULT NULL,
  `total_laid_off` int(11) DEFAULT NULL,
  `percentage_laid_off` text DEFAULT NULL,
  `date` text DEFAULT NULL,
  `stage` text DEFAULT NULL,
  `country` text DEFAULT NULL,
  `funds_raised_millions` int(11) DEFAULT NULL,
   `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company,location
,industry,total_laid_off,
percentage_laid_off,`date`,stage,country,funds_raised_millions) AS row_num # creating row_num to detect duplicates
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;          # deleting duplicates 

-- Standardizing data

UPDATE layoffs_staging2
SET company = TRIM(company);

SELECT *
FROM layoffs_staging2
WHERE industry LIKE "Crypto%"  # there is some repetitions with crypto, lets fix it!
ORDER BY 1;

UPDATE layoffs_staging2
SET industry = "Crypto"
WHERE industry LIKE "Crypto%";  # fixing the repetitions 


SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1;

SELECT  DISTINCT country
FROM layoffs_staging2  
ORDER BY 1;

SELECT  DISTINCT country
FROM layoffs_staging2		# a similar problem with united states, lets fix it!
WHERE country LIKE "United States%"; 

SELECT DISTINCT country, TRIM(TRAILING "." FROM country)
FROM layoffs_staging2                                       # testing the solution 
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING "." FROM country)
WHERE country LIKE "United States%";  # fixing the problem 

SELECT `date`
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, "%m/%d/%y"); # transforming the date column to be able to give it DATE as the data type


ALTER TABLE layoffs_staging2 
MODIFY COLUMN `date` DATE;   # changing data type

-- Null & Blank values

SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL   # detenting some NULLS
AND percentage_laid_off IS NULL;


UPDATE layoffs_staging2
SET industry = null   # doing some preparation to fix the problem 
WHERE industry = "";

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry = "";


SELECT t1.industry, t2.industry
FROM layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company   # testing the solution for the null problem in industry column 
    AND t1.location = t2.location
WHERE (t1.industry IS NULL OR t1.industry = "")
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 AS t1
JOIN layoffs_staging2 AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry   # executing the solution
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;


SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL  # I can't trust in this data so im going to delete 
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;


ALTER TABLE layoffs_staging2
DROP COLUMN row_num;  # I DON'T NEED THIS COLUMN ANYMORE EITHER, IT WAS JUST TO HELP ME DETECT DUPLICATES

# cleaned data 
SELECT *
FROM layoffs_staging2;