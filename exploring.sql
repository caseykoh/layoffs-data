-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

SELECT MAX(total_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE total_laid_off = (SELECT MAX(total_laid_off)
FROM layoffs_staging2);


ALTER TABLE layoffs_staging2
ADD COLUMN funds_raised_millions_int INT;

UPDATE layoffs_staging2
SET funds_raised_millions_int = ROUND(CAST(funds_raised_millions AS DECIMAL));

ALTER TABLE layoffs_staging2
DROP COLUMN funds_raised_millions;

ALTER TABLE layoffs_staging2
RENAME COLUMN funds_raised_millions_int TO funds_raised_millions;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1
ORDER BY funds_raised_millions DESC;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;

SELECT company, location, `date`, SUM(total_laid_off) OVER(PARTITION BY company) AS total_layoff
FROM layoffs_staging2
ORDER BY total_layoff DESC
;

SELECT MIN(`date`), MAX(`date`)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE `date` =
(SELECT MIN(`date`)
 FROM layoffs_staging2);
 
SELECT YEAR(`date`) AS year_layoff, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY year_layoff
ORDER BY 1 DESC
;

SELECT *
FROM layoffs_staging2
WHERE industry = 'Consumer'
ORDER BY total_laid_off DESC
;

SELECT stage, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY stage
ORDER BY 2 DESC
;

SELECT company, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC
;



-- Rolling sum 
SELECT SUBSTRING(`date`, 1, 7) AS month_date, SUM(total_laid_off)
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY month_date
ORDER BY 1 ASC
;

WITH Rolling_Total AS
(
SELECT SUBSTRING(`date`, 1, 7) AS month_date, SUM(total_laid_off) as total_month
FROM layoffs_staging2
WHERE SUBSTRING(`date`, 1, 7) IS NOT NULL
GROUP BY month_date
ORDER BY 1 ASC
)
SELECT month_date,
total_month,
SUM(total_month) OVER(ORDER BY month_date ASC) as rolling_total
FROM Rolling_Total
;

SELECT 
SUM(total_laid_off)
FROM layoffs_staging2
;

SELECT *, YEAR(`date`) AS year_date, SUM(total_laid_off) OVER(PARTITION BY YEAR(`date`)) AS year_total
FROM layoffs_staging2
WHERE company='Amazon'
;


WITH company_year(company, years, total_laid_off) AS
(
SELECT company, YEAR(`date`), SUM(total_laid_off) 
FROM layoffs_staging2
GROUP BY company, YEAR(`date`)
), Company_Year_Rank AS
(SELECT *, DENSE_RANK() OVER (PARTITION BY years ORDER BY total_laid_off DESC) AS Ranking
FROM company_year
WHERE years IS NOT NULL
)
SELECT *
FROM Company_Year_Rank
WHERE Ranking <= 5
;





