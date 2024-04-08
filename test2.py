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

# 查找每个FROM app.app_jira_cuss_jfs_result_da后面是否有WHERE条件，并添加条件
for i, line in enumerate(sql_lines):
    if 'FROM app.' in line:
        where_index = i + 1
        while where_index < len(sql_lines) and sql_lines[where_index].strip().startswith('--'):
            where_index += 1
        if where_index < len(sql_lines) and 'WHERE' not in sql_lines[where_index]:
            sql_lines.insert(where_index, f"WHERE dt='{today}'")
        elif where_index < len(sql_lines) and 'dt' not in sql_lines[where_index]:
            sql_lines[where_index] = sql_lines[where_index].replace('WHERE', f"WHERE dt='{today}' AND ")
    if 'fix_version =' in line:
        sql_lines[i]=sql_lines[i].replace('fix_version =','fix_version like')

# 重新组合SQL查询
modified_sql_query = '\n'.join(sql_lines)

# 打印并保存到txt文件中
print(modified_sql_query)
with open('modified_sql_query.txt', 'w') as f:
    f.write(modified_sql_query)
