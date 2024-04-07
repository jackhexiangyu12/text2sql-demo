import requests
import json

# prompt = "最近一天问题的分布是怎么样的？/哪类问题最多？有多少？/近一周，哪类问题增长最快？/v1.4版本上线后，什么问题比较多？"
#
# schema="""CREATE EXTERNAL TABLE `app.app_jira_cuss_jfs_result_da`(
#   `project_id` string COMMENT '项目ID',
#   `project_name` string COMMENT '项目名称',
#   `project_type` string COMMENT '项目类型',
#   `assignee_type` string COMMENT '项目类别',
#   `issue_id` string COMMENT 'issueID',
#   `issue_num` string COMMENT 'issue序号',
#   `summary` string COMMENT '概述',
#   `issue_type` string COMMENT 'issue类型',
#   `issue_status` string COMMENT 'issue状态',
#   `priority` string COMMENT '优先级',
#   `resolution` string COMMENT '解决结果',
#   `affects_versions` string COMMENT '影响版本',
#   `fix_version` string COMMENT 'issue修复版本',
#   `label_value` string COMMENT '固定标签内容',
#   `vehicle_vin_code` string COMMENT 'vin',
#   `vehicle_environment` string COMMENT '车辆环境',
#   `feedback_channels` string COMMENT '反馈渠道',
#   `user_feedback_flink` string COMMENT '用户反馈链接',
#   `user_feedback_flink_id` string COMMENT '用户反馈链接id',
#   `issue_desc` string COMMENT 'issue描述',
#   `issue_category` string COMMENT '问题类别',
#   `issue_second_category` string COMMENT '问题二级类别',
#   `issue_create_time` string COMMENT '创建时间',
#   `issue_update_time` string COMMENT '更新时间',
#   `resolution_time` string COMMENT '解决时间',
#   `first_feedback_time` string COMMENT '第一次反馈时间',
#   `vehicle_active_status_desc` string COMMENT '车辆激活状态描述(未激活、激活、激活功能关闭)',
#   `vehicle_category_desc` string COMMENT '车辆类型描述(模拟车、台架车、试制车)',
#   `vehicle_status_desc` string COMMENT '车辆状态描述(生产中、待交付、已交付、已报废)',
#   `actual_shop_name` string COMMENT '门店名称',
#   `actual_city_name` string COMMENT '门店城市名称',
#   `actual_region_name` string COMMENT '门店大区名称',
#   `change_duration_s` string COMMENT '',
#   `vehicle_usemodel_desc` string COMMENT '车辆用途描述',
#   `root_cause` string COMMENT 'Root Cause',
#   `long_term_solution` string COMMENT '长期解决方案',
#   `duplicate_source` string COMMENT '来源',
#   `duplicated_with` string COMMENT '关联',
#   `group_id` string COMMENT 'group_id',
#   `field_type` string COMMENT 'field_type',
#   `field` string COMMENT 'field',
#   `old_value` string COMMENT 'old_value',
#   `old_string` string COMMENT 'old_string',
#   `new_value` string COMMENT 'new_value',
#   `new_string` string COMMENT 'new_string',
#   `current_province_name` string COMMENT '当日凌晨12点所处省份名称',
#   `current_city_name` string COMMENT '当日凌晨12点所处城市名称',
#   `current_district_name` string COMMENT '当日凌晨12点所处街道名称',
#   `current_address` string COMMENT '当日凌晨12点所处详细地址',
#   `current_province_code` string COMMENT '当日凌晨12点所处省份编码',
#   `current_city_code` string COMMENT '当日凌晨12点所处城市编码',
#   `current_district_code` string COMMENT '当日凌晨12点所处街道编码',
#   `location_update_time` string COMMENT '车辆位置更新时间',
#   `duplicate_heat_value` string COMMENT '热度值',
#   `duplicate_issue_list` string COMMENT '问题清单',
#   `order_id` string COMMENT '工单id',
#   `channel_id` string COMMENT '客服渠道id',
#   `cuss_issue_id` string COMMENT 'issue_id',
#   `cuss_issue_name` string COMMENT '问题类型名称',
#   `cuss_issue_level` string COMMENT '问题类型级别',
#   `order_status` string COMMENT '客服工单状态',
#   `order_status_desc` string COMMENT '客服工单状态描述',
#   `order_content` string COMMENT '客服工单内容',
#   `order_label` string COMMENT '客服工单标签',
#   `finish_time` string COMMENT '完成时间',
#   `cuss_create_time` string COMMENT '创建时间',
#   `cuss_update_time` string COMMENT '更新时间',
#   `file_time` string COMMENT '归档时间',
#   `cuss_issue_pid` string COMMENT 'pid',
#   `channel_desc` string COMMENT '客服渠道描述',
#   `create_type` string COMMENT '创建类型',
#   `create_type_desc` string COMMENT '创建类型描述',
#   `resident_province_name` string COMMENT '常驻省名称',
#   `resident_city_name` string COMMENT '常驻城市名称',
#   `assignee_account` string COMMENT '经办人账号',
#   `assignee` string COMMENT '经办人',
#   `reporter_account` string COMMENT '报告人账号',
#   `reporter` string COMMENT '报告人')
# COMMENT 'jira-jfs-用户车辆反馈结果表'
# PARTITIONED BY (
#   `dt` string)
# ROW FORMAT SERDE
#   'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
# STORED AS INPUTFORMAT
#   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
# OUTPUTFORMAT
#   'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
# LOCATION
#   'hdfs://jdhadoop/user/hive/warehouse/app.db/app_jira_cuss_jfs_result_da'
# TBLPROPERTIES (
#   'bucketing_version'='2',
#   'last_modified_by'='jd-dw',
#   'last_modified_time'='1709610603',
#   'transient_lastDdlTime'='1709610603')"""
#
# # 设置请求的URL和headers
# url = "https://www.text2sql.ai/api/sql/generate"
# headers = {
#     "Authorization": "Bearer e30381a8ba1f85bbe3b1585cc61ef2884dbc26a27b79775898de16b1afdfa5c8",
#     "Content-Type": "application/json"
# }
#
#
# # 设置请求的数据
# data = {
#     "prompt": prompt,
#     "type": "hive",
#     "schema": schema
# }
#
# # 发送POST请求
# response = requests.post(url, headers=headers, data=json.dumps(data))
#
# # 打印响应内容
# print(response.text)

# 解析json内容
response_text = '{"output":"-- 最近一天问题的分布\\nSELECT issue_category, COUNT(*) AS total_count\\nFROM app.app_jira_cuss_jfs_result_da\\nWHERE dt = DATE_SUB(CURRENT_DATE, 1)\\nGROUP BY issue_category;\\n\\n-- 哪类问题最多？有多少？\\nSELECT issue_category, COUNT(*) AS total_count\\nFROM app.app_jira_cuss_jfs_result_da\\nGROUP BY issue_category\\nORDER BY total_count DESC\\nLIMIT 1;\\n\\n-- 近一周，哪类问题增长最快？\\nSELECT issue_category, COUNT(*) AS total_count\\nFROM app.app_jira_cuss_jfs_result_da\\nWHERE dt BETWEEN DATE_SUB(CURRENT_DATE, 7) AND CURRENT_DATE\\nGROUP BY issue_category\\nORDER BY total_count DESC\\nLIMIT 1;\\n\\n-- v1.4版本上线后，什么问题比较多？\\nSELECT issue_category, COUNT(*) AS total_count\\nFROM app.app_jira_cuss_jfs_result_da\\nWHERE fix_version = \'v1.4\'\\nGROUP BY issue_category\\nORDER BY total_count DESC\\nLIMIT 1;","generation":{"count":2}}'
# 解析JSON字符串
parsed_data = json.loads(response_text)

# 获取output字段的内容
sql_queries = parsed_data['output']

# 将内容写入txt文件
with open('sql_queries.txt', 'w', encoding='utf-8') as file:
    file.write(sql_queries)

# 打印完成消息
print("SQL查询已成功保存到sql_queries.txt文件中。")