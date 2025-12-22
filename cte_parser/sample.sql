-- 示例SQL文件，包含多种WITH语句（CTE）

-- 单个CTE
WITH customer_orders AS (
    SELECT customer_id, COUNT(*) as order_count
    FROM orders
    WHERE order_date >= '2024-01-01'
    GROUP BY customer_id
)
SELECT * FROM customer_orders WHERE order_count > 5;

-- 多个CTE
WITH 
    sales_data AS (
        SELECT 
            product_id,
            SUM(amount) as total_sales
        FROM sales
        GROUP BY product_id
    ),
    product_info AS (
        SELECT 
            id,
            name,
            category
        FROM products
    ),
    top_products AS (
        SELECT 
            p.name,
            s.total_sales
        FROM sales_data s
        JOIN product_info p ON s.product_id = p.id
        WHERE s.total_sales > 10000
    )
SELECT * FROM top_products ORDER BY total_sales DESC;

-- 递归CTE
WITH RECURSIVE employee_hierarchy AS (
    SELECT id, name, manager_id, 1 as level
    FROM employees
    WHERE manager_id IS NULL
    
    UNION ALL
    
    SELECT e.id, e.name, e.manager_id, eh.level + 1
    FROM employees e
    INNER JOIN employee_hierarchy eh ON e.manager_id = eh.id
)
SELECT * FROM employee_hierarchy;

-- 嵌套查询中的CTE
WITH regional_sales AS (
    SELECT region, SUM(amount) as total
    FROM orders
    GROUP BY region
),
top_regions AS (
    SELECT region
    FROM regional_sales
    WHERE total > (SELECT AVG(total) FROM regional_sales)
)
SELECT r.region, r.total
FROM regional_sales r
WHERE r.region IN (SELECT region FROM top_regions);

-- 带注释的CTE
WITH 
    -- 活跃用户统计
    active_users AS (
        SELECT user_id, last_login
        FROM users
        WHERE last_login >= CURRENT_DATE - INTERVAL '30 days'
    ),
    /* 订单统计 */
    user_orders AS (
        SELECT user_id, COUNT(*) as order_count
        FROM orders
        GROUP BY user_id
    )
SELECT 
    au.user_id,
    uo.order_count
FROM active_users au
LEFT JOIN user_orders uo ON au.user_id = uo.user_id;
