select *
from  layoffs;

-- 1 Remove duplicates 
-- 2 standardize the data 
-- 3 null value or blank values 
-- 4 remove any col 

-- copy for saving the original database
create table layoffs_staging 
like layoffs;

select * 
from layoffs_staging ;

insert layoffs_staging
select * 
from layoffs;


select * 
from layoffs_staging;


select * ,
ROW_NUMBER() OVER(
partition by company, location, industry,total_laid_off,percentage_laid_off,`date`) as row_num
from layoffs_staging;

-- assign row num for getting the duplicates

with duplicate_cte as 
(
select * ,
ROW_NUMBER() OVER(
partition by company,location,
 industry,total_laid_off,percentage_laid_off,`date`,stage,
 country,funds_raised_millions) as row_num
from layoffs_staging
)
select * 
from duplicate_cte 
where row_num > 1 ;



select * 
from layoffs_staging
where company = 'Casper';







-- create 3 copy of data

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

select * 
from layoffs_staging2;


INSERT INTO layoffs_staging2
select *,
ROW_NUMBER() OVER(
partition by company,location,
 industry,total_laid_off,percentage_laid_off,`date`,stage,
 country,funds_raised_millions) as row_num
from layoffs_staging;

select * 
from layoffs_staging2;


-- deleting  the duplicates 
delete
from layoffs_staging2
where row_num > 1;


select * 
from layoffs_staging2
where row_num > 1;


-- standardizing data 

select company, trim(company)
from layoffs_staging2 ;



update layoffs_staging2
set company = trim(company);

-- selected industry check for distinct 

select distinct industry
from layoffs_staging2;

-- checked for the same name 
-- updated to one name 

select *
from layoffs_staging2
where industry like 'crypto%';


update layoffs_staging2
set industry = 'Crypto'
where industry like  'crypto%';


select distinct industry
from layoffs_staging2;



select distinct location 
from layoffs_staging2
order by 1 ;


select distinct country 
from layoffs_staging2
order by 1 ;

select distinct country , trim(trailing '.' from country)
from layoffs_staging2
-- where country like 'United States%'
order by 1 ;


UPDATE layoffs_staging2
set country = trim(trailing '.' from country) 
where country like 'United States%';



select * 
from layoffs_staging2;
select `date` 
FROM layoffs_staging2;

select `date` ,
str_to_date(`date`,'%m/%d/%Y')
FROM layoffs_staging2;

update layoffs_staging2
set `date`	= str_to_date(`date`,'%m/%d/%Y');

ALTER TABLE layoffs_staging2
MODIFY COLUMN `date` DATE ;	


-- checking about the null values both are nulls
select * 
from layoffs_staging2
where total_laid_off is null 
and percentage_laid_off is null ;

-- empty and null
select *
from layoffs_staging2 
where industry is null 
or industry = ''; 


-- c first checked the value is empty of same table with each other comparison 
-- then we converted empty to null then null to value 
select * 
from layoffs_staging2
where company = 'Airbnb' ;

select * 
from layoffs_staging2 T1
join layoffs_staging2 T2 
    on T1.company = T2.company 
where (T1.industry  is null  or T1.industry = '')
and T2.industry is not null ;


update layoffs_staging2 T1 
join layoffs_staging2 T2 
    on T1.company = T2.company 
    set T1.industry = T2.industry
    where (T1.industry  is null  or T1.industry = '')
and T2.industry is not null ;



update layoffs_staging2 
set industry = null 
where industry = '';

select * 
from layoffs_staging2
where company = 'Airbnb';


update layoffs_staging2 T1 
join layoffs_staging2 T2 
    on T1.company = T2.company 
    set T1.industry = T2.industry
    where T1.industry  is null  
and T2.industry is not null ;




select * 
from layoffs_staging2
where company  like 'Bally%';



select * 
from layoffs_staging2 ;


select * 
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null ;


delete 
from layoffs_staging2 
where total_laid_off is null 
and percentage_laid_off is null ;

select * 
from layoffs_staging2;


Alter table layoffs_staging2
drop column row_num;


