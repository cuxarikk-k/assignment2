drop database assignment2;
create database assignment2;
use assignment2;

drop table cocktails;
drop table ingredients;
drop table recipeUsage;
-- table creation(1m, 1m, 3m)
create table cocktails (
	cocktail_id int primary key,
    name varchar(100) not null,
    category varchar(50) not null,
    base varchar(120) not null,
    difficulty_score int,
    popularity_score decimal(3, 2),
    preparation_date date
);

create table ingredients (
	ingredient_id int primary key,
    name varchar(50) not null,
	quality_score decimal(3, 2),
    type varchar(50) not null,
    cost_usd decimal(8, 2),
    quantity int
);

create table recipeUsage (
	usage_id bigint primary key,
    cocktail_id int not null,
    ingredient_id int not null,
    volumeMl int,
    usage_date datetime default current_timestamp,
    foreign key (cocktail_id) references cocktails(cocktail_id),
    foreign key (ingredient_id) references ingredients(ingredient_id)
);

-- non-optimised query
-- explain analyze
SELECT 
    c.name, 
    AVG(c.popularity_score) AS global_popularity,
    SUM(ru.volumeMl * i.cost_usd) AS total_recipe_cost
FROM 
    cocktails c
JOIN 
    recipeUsage ru ON c.cocktail_id = ru.cocktail_id
JOIN 
    ingredients i ON ru.ingredient_id = i.ingredient_id
CROSS JOIN (
    SELECT AVG(cost_usd) AS global_cost
    FROM ingredients
) AS avg_cost
WHERE 
    c.category = 'classic'
    AND ru.usage_date >= '2024-10-01' 
    AND i.cost_usd > avg_cost.global_cost
GROUP BY 
    c.cocktail_id, c.name
ORDER BY 
    global_popularity DESC,
    total_recipe_cost desc
LIMIT 10;

-- indexes
create index idx_cocktails_category_popularity
on cocktails(category, popularity_score); -- makes quicker WHERE c.category and ORDER BY popularity score
drop index idx_cocktails_category_popularity on cocktails;

create index idx_ingredient_cost
on ingredients(cost_usd); -- makes quicker scan for i.cost_usd > averege_cost
drop index idx_ingredient_cost on indredients;

create index idx_cocktails_usage_date
on recipeUsage(cocktail_id, usage_date); -- more effective JOIN on cocktail_id and scan on usage_date
drop index idx_cocktails_usage_date on recipeUsage;

-- optimised query
-- explain analyze
with averageCost as (
	select avg(cost_usd) as global_cost
	from ingredients
) -- cte
select 
	/*+ JOIN_FIXED_ORDER(a, i, r, c) */
	c.name, 
    avg(c.popularity_score) as global_popularity,
    sum(r.volumeMl * i.cost_usd) as total_recipe_cost
from cocktails c use index (idx_cocktails_category_popularity)
join recipeUsage r use index (idx_cocktails_usage_date)
on c.cocktail_id = r.cocktail_id
join ingredients i on r.ingredient_id = i.ingredient_id
join averageCost a
where
	c.category = 'classic'
    and r.usage_date >= '2024-10-01' -- rewriting - change date_sub to constant date for index range scan
    and i.cost_usd > a.global_cost
group by c.cocktail_id, c.name
order by global_popularity desc, total_recipe_cost desc
limit 10;

-- The core issue was slow query performance on millions of rows due to inefficient calculation and scanning. Our goal was to achieve identical results but at high speed.

-- Key Optimization Methods:
-- CTEs (Common Table Expressions): We replaced the slow, repeated subquery for AVG(cost_usd) with a WITH block. This forces the average to be calculated once and materialized, converting a bottleneck into a simple, fast JOIN. 
-- Query Rewriting: We ensured the date filter uses a static constant ('2024-10-01'). This allows the optimizer to perform a rapid Index Range Scan on our composite index, rather than a full table scan.
-- Targeted Indexing: We created three specific indexes to support every WHERE, JOIN, and ORDER BY condition.
-- Bonus Hint: We added /*+ JOIN_FIXED_ORDER(...) */ to override the optimizer and guarantee the best possible join sequence, starting with the smallest, pre-calculated results.

-- Conclusion:
-- The EXPLAIN ANALYZE confirms our success: the optimized query shifts execution from slow Full Scans and filesort to high-speed Index Range Access.
