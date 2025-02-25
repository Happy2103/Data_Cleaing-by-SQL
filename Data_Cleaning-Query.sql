-- DATA CLEANING
select * from layoffs;

-- STEPS FOLLOWED
-- 1. Remove Duplicates
-- 2. Standardize the Data
-- 3. Check Null values or Blank Values 
-- 4. Remove any columns

-- Step1 Creating a staging Table
create table layoffs_staging
Like layoffs;

-- Importing Data from main table
insert layoffs_staging
select *
from layoffs;

select * from layoffs_staging;

----------------------------------------------------------------- STEP 1: Removing Duplicates---------------------------------------------------------------------------
select * ,ROW_NUMBER() over(
PARTITION BY company,industry,total_laid_off,`date`) as row_num
from layoffs_staging;

-- A:Checking duplicates using CTE

With duplicate_cte as(
select * ,ROW_NUMBER() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte
where row_num >1;

-- B: Deleting Duplicates
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
  `row_num` int
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

insert into layoffs_staging2
select * ,ROW_NUMBER() over(
PARTITION BY company,location,industry,total_laid_off,percentage_laid_off,`date`,stage,country,funds_raised_millions) as row_num
from layoffs_staging;

select * 
from layoffs_staging2
where row_num >1;

DELETE
from layoffs_staging2
where row_num >1;

-- ----------------------------------------------------------------- STEP 2: Standardzing DATA-------------------------------------------------------------------------

-- A: Removing White Space

update layoffs_staging2
set company = trim(company);

select company,trim(company)
from layoffs_staging2;

-- B: Making similar Industry name 
select distinct industry
from layoffs_staging2;

select *
from layoffs_staging2
where industry like 'crypto%';

update layoffs_staging2
set industry  = 'crypto'
where industry like 'crypto%';

select distinct country
from layoffs_staging2
order by 1;

update layoffs_staging2
set country ='United States'
where country like 'United States%';


-- C: Formating Date 

select `date`,
STR_TO_DATE( `date`, '%m/%d/%Y')
from layoffs_staging2;

update layoffs_staging2
set `date` = STR_TO_DATE( `date`, '%m/%d/%Y');

-- Change Datatype of Date Column
Alter Table layoffs_staging2
modify column `date` DATE;


-- ----------------------------------------------------------------- STEP 3: Check and Handle NULL Values-------------------------------------------------------------------------

select *
from layoffs_staging2
where industry is null
or industry = '';

-- This wil compare the industry which are null and not null

select t1.industry, t2.industry
from layoffs_staging2 as t1
join layoffs_staging2 as t2
   on t1.company = t2.company
where t1.industry is null 
and t2.company is not null;

update layoffs_staging2
set industry = null
where industry = '';

update layoffs_staging2 as t1
join layoffs_staging2 as t2
   on t1.company = t2.company
set t1.industry = t2.industry
where t1.industry is null  
and t2.company is not null;


-- Deleting Null values rows
select * 
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

Delete
from layoffs_staging2
where total_laid_off is null
and percentage_laid_off is null;

select * from layoffs_staging2