-- Data Cleaning
SELECT *
FROM layoffs;

-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Null Values or blank values
-- 4. Remove Any Columns

-- Make a copy of layoffs into layoffs_staging 
CREATE TABLE layoffs_staging
LIKE layoffs;  

SELECT *
FROM layoffs_staging;

INSERT layoffs_staging
SELECT *
FROM layoffs;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, industry, total_laid_off, percentage_laid_off, `date`) AS row_num
FROM layoffs_staging
;

WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


WITH duplicate_cte AS (
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging
)
SELECT *
FROM duplicate_cte
WHERE row_num > 1;


CREATE TABLE `layoffs_staging2` (
  `company` text,
  `location` text,
  `industry` text,
  `total_laid_off` text,
  `percentage_laid_off` text,
  `date` text,
  `stage` text,
  `country` text,
  `funds_raised_millions` text,
  `row_num` INT
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

SELECT *
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location,
industry, total_laid_off, percentage_laid_off, `date`, stage, 
country, funds_raised_millions) AS row_num
FROM layoffs_staging;

DELETE
FROM layoffs_staging2
WHERE row_num > 1;

-- Standardizing data
SELECT *
FROM layoffs_staging2
;

SELECT company, TRIM(company)
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET company = TRIM(company)
;

SELECT DISTINCT industry
FROM layoffs_staging2
ORDER BY 1
;

SELECT *
FROM layoffs_staging2
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT DISTINCT COUNTRY
FROM layoffs_staging2
ORDER BY 1
;

SELECT *
FROM layoffs_staging2
WHERE country LIKE 'United States'
;

SELECT *,
ROW_NUMBER() OVER(
PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions) AS row_num
FROM layoffs_staging;

WITH US_cte AS (
	SELECT *
	FROM layoffs_staging2
	WHERE country LIKE 'United States%'
)
SELECT *, ROW_NUMBER() OVER(
PARTITION BY country
) AS row_num
FROM US_cte
WHERE country != 'United States';

UPDATE layoffs_staging2
SET country = 'United States'
WHERE country LIKE 'United States%'
AND country != 'United States';

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_staging2
ORDER BY 1;

UPDATE layoffs_staging2
SET country = TRIM(TRAILING '.' FROM country)
WHERE country LIKE 'United States%'
;

SELECT `date`
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE `date` = NULL;

UPDATE layoffs_staging2
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y');

UPDATE layoffs_staging2
SET `date` = 
    CASE
        WHEN `date` = 'NULL' THEN NULL
        ELSE STR_TO_DATE(`date`, '%m/%d/%Y')
    END;
    
SELECT *
FROM layoffs_staging2
WHERE company = 'Alerzo';

UPDATE layoffs_staging2
SET total_laid_off = NULL
WHERE company = 'Alerzo'
AND total_laid_off = 'NULL'
;

UPDATE layoffs_staging2
SET 
    location = CASE WHEN location = 'NULL' THEN NULL ELSE location END,
    industry = CASE WHEN industry = 'NULL' THEN NULL ELSE industry END,
    total_laid_off = CASE WHEN total_laid_off = 'NULL' THEN NULL ELSE total_laid_off END,
    percentage_laid_off = CASE WHEN percentage_laid_off = 'NULL' THEN NULL ELSE percentage_laid_off END,
    `date` = CASE WHEN `date` = 'NULL' THEN NULL ELSE `date` END,
    stage = CASE WHEN stage = 'NULL' THEN NULL ELSE stage END,
    country = CASE WHEN country = 'NULL' THEN NULL ELSE country END,
    funds_raised_millions = CASE WHEN funds_raised_millions = 'NULL' THEN NULL ELSE funds_raised_millions END;


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;

SELECT *
FROM layoffs_staging2;

-- Deal with nulls, blanks
SELECT *
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

SELECT DISTINCT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry='';

SHOW COLUMNS FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off REGEXP '[^0-9]'
OR funds_raised_millions REGEXP '[^0-9]';

UPDATE layoffs_staging2
SET total_laid_off = CAST(total_laid_off AS UNSIGNED)
WHERE total_laid_off IS NOT NULL;

UPDATE layoffs_staging2
SET funds_raised_millions = CAST(funds_raised_millions AS UNSIGNED)
WHERE funds_raised_millions IS NOT NULL;

ALTER TABLE layoffs_staging2
MODIFY total_laid_off INT;


