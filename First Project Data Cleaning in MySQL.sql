-- CREATE BACKUP TABLE TO SAVE THE ROW DATA 

CREATE TABLE layoffs_ba
(
select *
from layoffs
);

SELECT *
FROM layoffs_ba
;

-- Remove doblic

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions)
FROM layoffs_ba;


WITH layoffs_ba_dp AS 
(
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_nu
FROM layoffs_ba
)

SELECT *
FROM layoffs_ba_dp
WHERE row_nu > 1
;

SELECT *
FROM layoffs_ba
WHERE company = 'Casper'
;

CREATE TABLE layoffs_ba_dp
SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_nu
FROM layoffs_ba;

SELECT *
FROM layoffs_ba_dp
;

-- 2. STANDARDIZING DATA

SELECT company
FROM layoffs_ba_dp
;

UPDATE layoffs_ba_dp
SET company = trim(company);

SELECT DISTINCT(location)
FROM layoffs_ba_dp
ORDER BY 1
;

SELECT DISTINCT(industry)
FROM layoffs_ba_dp
ORDER BY 1
;

SELECT *
FROM layoffs_ba_dp
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_ba_dp
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%';

SELECT `date`,
STR_TO_DATE(`DATE` , '%m/%d/%Y')
FROM layoffs_ba_dp;

UPDATE layoffs_ba_dp
SET `DATE` = STR_TO_DATE(`DATE` , '%m/%d/%Y');

ALTER TABLE layoffs_ba_dp
MODIFY COLUMN `DATE` DATE;

SELECT *
FROM layoffs_ba_dp
;

SELECT distinct(country)
FROM layoffs_ba_dp
ORDER BY 1
;

SELECT distinct country,TRIM(TRAILING '.' FROM country)
FROM layoffs_ba_dp
ORDER BY 1
;

UPDATE layoffs_ba_dp
SET country = TRIM(TRAILING '.' FROM country);

SELECT *
FROM layoffs_ba_dp
;

-- 3. Null values or blank values

SELECT *
FROM layoffs_ba_dp
WHERE total_laid_off IS NULL 
AND percentage_laid_off IS NULL 
AND funds_raised_millions IS NULL
;

SELECT *
FROM layoffs_ba_dp
WHERE industry IS NULL
OR industry = '';

SELECT *
FROM layoffs_ba_dp
WHERE company = 'Airbnb';

SELECT t1.company,t1.industry,t2.company,t2.industry
FROM layoffs_ba_dp AS t1
JOIN layoffs_ba_dp AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
    WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL
    ;
    
UPDATE layoffs_ba_dp AS t1
JOIN layoffs_ba_dp AS t2
	ON t1.company = t2.company
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
    AND t2.industry IS NOT NULL
    ;
    
UPDATE layoffs_ba_dp
SET industry = NULL
WHERE industry = '';
    
SELECT *
FROM layoffs_ba_dp
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL 
;

DELETE
FROM layoffs_ba_dp
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL 
;

SELECT *
FROM layoffs_ba_dp;

-- 4. Remove any unnecessary calumns  

SELECT *
FROM layoffs_ba_dp;

ALTER TABLE layoffs_ba_dp
DROP COLUMN row_nu;
