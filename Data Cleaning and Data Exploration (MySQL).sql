-- 1. Remove Duplicates

CREATE TABLE layoffs_staging
LIKE layoffs;

INSERT layoffs_staging
SELECT *
FROM layoffs;

select *
from layoffs_staging;


WITH duplicate_cte AS
(
SELECT *,
ROW_NUMBER() OVER(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging
)

DELETE
FROM duplicate_cte
WHERE row_num > 1;

SELECT *
FROM layoffs_staging
WHERE company = 'Cazoo' ;


CREATE TABLE `layoffs_staging2` (
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
FROM layoffs_staging2
WHERE row_num > 1;

INSERT INTO layoffs_staging2
SELECT *,
ROW_NUMBER() OVER(
 PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, `date`, stage, country, funds_raised_millions ) AS row_num
FROM layoffs_staging;

SELECT *
FROM layoffs_staging2;



-- 2. Standardize the Data

SELECT  company, (TRIM(company))
FROM layoffs_staging2;

UPDATE layoffs_staging2
SET company = TRIM (company) ; 

SELECT  DISTINCT Country
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

UPDATE layoffs_staging2
SET Country =  TRIM(TRAILING ' . ' FROM country)
WHERE industry LIKE 'United States%';


SELECT  	`date`
FROM layoffs_staging2
;

UPDATE layoffs_staging2
SET 	`date` = STR_TO_DATE(`date`, '%m/%d/%Y');


ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE;



-- 3. Null Values or blank values

SELECT *
FROM layoffs_staging2
WHERE industry IS NULL
OR industry =  ' ' ;

SELECT t1.industry, t2.industry
FROM layoffs_staging2 t1
JOIN layoffs_staging2 t2
	on t1.company = t2.company
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

UPDATE layoffs_staging2 t1
JOIN layoffs_staging2 t2
	ON t1.company = t2.company
SET  t1.industry = t2.industry
WHERE t1.industry IS NULL 
AND t2.industry IS NOT NULL;

SELECT * 
FROM layoffs_staging2
WHERE company = 'Airbnb';


-- 4. Remove Any Columns

SELECT * 
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

DELETE
FROM layoffs_staging2
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL;

ALTER TABLE layoffs_staging2
DROP COLUMN row_num;




-- Exploratory Data Analysis

SELECT *
FROM layoffs_staging2;

SELECT  MAX(total_laid_off), MAX(percentage_laid_off)
FROM layoffs_staging2;

SELECT *
FROM layoffs_staging2
WHERE percentage_laid_off = 1 
ORDER BY funds_raised_millions DESC;

SELECT  company, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT  MIN(`DATE`), MAX(`DATE`)
FROM layoffs_staging2;

SELECT  country, SUM(total_laid_off)
FROM layoffs_staging2
GROUP BY country
ORDER BY 2 DESC;

SELECT  YEAR(`DATE`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY   YEAR(`DATE`)
ORDER BY 1 DESC;

SELECT  stage, sum(total_laid_off)
FROM layoffs_staging2
GROUP BY   stage
ORDER BY 2 DESC;

SELECT  company, AVG(percentage_laid_off)
FROM layoffs_staging2
GROUP BY company
ORDER BY 2 DESC;

SELECT substring(`date`, 6, 2) AS `MONTH`, SUM(total_laid_off)
FROM layoffs_staging2
WHERE substring(`date`, 6, 2) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC;

WITH Rolling_Total AS
(
SELECT substring(`date`, 6, 2) AS `MONTH`, SUM(total_laid_off) AS total_off
FROM layoffs_staging2
WHERE substring(`date`, 6, 2) IS NOT NULL
GROUP BY MONTH
ORDER BY 1 ASC
)
SELECT `MONTH`, SUM(total_off) OVER(ORDER BY `MONTH` ) AS rolling_total
from Rolling_Total;


SELECT  company, YEAR(`DATE`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`DATE`)
ORDER BY 3 DESC;


WITH COMPANY_YEAR (company, years , total_laid_off)AS
(
SELECT  company, YEAR(`DATE`), sum(total_laid_off)
FROM layoffs_staging2
GROUP BY company,YEAR(`DATE`)
), Company_year_Rank AS (
SELECT *, DENSE_RANK() OVER(PARTITION BY  years ORDER BY total_laid_off DESC) AS Ranking
FROM COMPANY_YEAR
Where years IS NOT NULL)

SELECT * 
FROM Company_year_Rank 
WHERE Ranking <= 5
;























