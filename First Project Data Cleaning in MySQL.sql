-- CREATE BACKUP TABLE 

SELECT *
FROM layoffs;

CREATE TABLE layoffs_ba
(
SELECT *
FROM layoffs
);

SELECT *
FROM layoffs_ba;

-- Remove duplicates

SELECT *,
ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
FROM layoffs_ba;

WITH layoffs_row AS
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_ba
)
SELECT *
FROM layoffs_row
WHERE row_num > 1
;

SELECT *
FROM layoffs_ba
WHERE company = 'Hibob'
;

CREATE TABLE layoffs_row_num
(
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY company, location, industry, total_laid_off, percentage_laid_off, date, stage, country, funds_raised_millions) AS row_num
	FROM layoffs_ba
);

SELECT *
FROM layoffs_row_num
WHERE row_num > 1;

DELETE
FROM layoffs_row_num
WHERE row_num > 1;

SELECT *
FROM layoffs_row_num
WHERE row_num > 1;

-- 2. STANDARDIZING DATA

SELECT company
FROM layoffs_row_num
;

UPDATE layoffs_row_num
SET company = TRIM(company);

SELECT DISTINCT industry
FROM layoffs_row_num
ORDER BY industry
;

SELECT *
FROM layoffs_row_num
WHERE industry LIKE 'Crypto%'
;

UPDATE layoffs_row_num
SET industry = 'Crypto'
WHERE industry LIKE 'Crypto%'
;

SELECT *
FROM layoffs_row_num
;

SELECT `date`,
STR_TO_DATE(`date`, '%m/%d/%Y')
FROM layoffs_row_num
;

UPDATE layoffs_row_num
SET `date` = STR_TO_DATE(`date`, '%m/%d/%Y')
;

ALTER TABLE layoffs_row_num
MODIFY COLUMN `date` DATE;

SELECT DISTINCT country
FROM layoffs_row_num
ORDER BY country
;

SELECT DISTINCT country, TRIM(TRAILING '.' FROM country)
FROM layoffs_row_num
ORDER BY country
;

UPDATE layoffs_row_num
SET country = TRIM(TRAILING '.' FROM country)
;

SELECT *
FROM layoffs_row_num
;

-- 3. Null values or blank values

SELECT *
FROM layoffs_row_num
WHERE industry IS NULL 
OR industry = ''
;

SELECT *
FROM layoffs_row_num
WHERE industry IS NULL 
OR industry = ''
;

UPDATE layoffs_row_num
SET industry = NULL
WHERE industry = '';

SELECT *
FROM layoffs_row_num AS t1
JOIN layoffs_row_num AS t2
	ON t1.company = t2.company
    AND t1.location = t2.location
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL
;

UPDATE layoffs_row_num AS t1
JOIN layoffs_row_num AS t2
ON t1.company = t2.company
AND t1.location = t2.location
SET t1.industry = t2.industry
WHERE t1.industry IS NULL
AND t2.industry IS NOT NULL;

SELECT *
FROM layoffs_row_num
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

DELETE 
FROM layoffs_row_num
WHERE total_laid_off IS NULL
AND percentage_laid_off IS NULL
;

SELECT *
FROM layoffs_row_num;

-- 4. Remove any unnecessary calumns  

SELECT *
FROM layoffs_row_num;

ALTER TABLE layoffs_row_num
DROP COLUMN row_num;

SELECT *
FROM layoffs_row_num;
