from datetime import datetime

# 原始的SQL查询
sql_query = """
-- 最近一天问题的分布
SELECT issue_category, COUNT(*) AS total_count
FROM app.app_jira_cuss_jfs_result_da
GROUP BY issue_category;

-- 哪类问题最多？有多少？
SELECT issue_category, COUNT(*) AS total_count
FROM app.app_jira_cuss_jfs_result_da
GROUP BY issue_category
ORDER BY total_count DESC
LIMIT 1;

-- 近一周，哪类问题增长最快？
SELECT issue_category, COUNT(*) AS total_count
FROM app.app_jira_cuss_jfs_result_da
WHERE dt BETWEEN DATE_SUB(CURRENT_DATE, 7) AND CURRENT_DATE
GROUP BY issue_category
ORDER BY total_count DESC
LIMIT 1;

-- v1.4版本上线后，什么问题比较多？
SELECT issue_category, COUNT(*) AS total_count
FROM app.app_jira_cuss_jfs_result_da
WHERE fix_version = 'v1.4'
GROUP BY issue_category
ORDER BY total_count DESC
LIMIT 1;
"""

# 今天的日期
today = datetime.now().strftime('%Y-%m-%d')

# 按行分割SQL查询
sql_lines = sql_query.split('\n')

# 查找FROM app.app_jira_cuss_jfs_result_da后面是否有WHERE条件
where_index = -1
for i, line in enumerate(sql_lines):
    if 'FROM app.app_jira_cuss_jfs_result_da' in line:
        where_index = i + 1
        break

# 如果FROM app.app_jira_cuss_jfs_result_da后面没有WHERE条件，就在FROM app.app_jira_cuss_jfs_result_da后面加上where dt=今天
if where_index < len(sql_lines) and 'WHERE' not in sql_lines[where_index]:
    sql_lines[where_index] = f"WHERE dt='{today}'"
# 如果有WHERE但是where的一行中不包含dt，那么在where中加上and dt=今天
elif where_index < len(sql_lines) and 'dt' not in sql_lines[where_index]:
    sql_lines[where_index] = sql_lines[where_index].replace('WHERE', f"WHERE dt='{today}' AND ")

# 重新组合SQL查询
modified_sql_query = '\n'.join(sql_lines)

# 打印并保存到txt文件中
print(modified_sql_query)
with open('modified_sql_query.txt', 'w') as f:
    f.write(modified_sql_query)
