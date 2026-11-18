-- ClickHouse 容器初始化(仅在数据目录为空时执行一次, 见官方镜像 /docker-entrypoint-initdb.d)。
--
-- 设计约束(clickhouse-storage-design.md §4.12): 应用不执行 CREATE DATABASE,
-- 目标库必须由部署预建, 因此生产库 tsp 与测试库 tsp_test 都在这里建好。
--
-- 账号不在 SQL 里创建: 官方镜像的 users.xml 存储只读, 初始化阶段执行
-- CREATE USER 会报 ACCESS_STORAGE_READONLY。用户由 compose 的
-- CLICKHOUSE_USER / CLICKHOUSE_PASSWORD 在建库前创建, 并自带 *.* 全量授权
-- (含 CREATE TABLE / DROP TABLE), 因此这里也不需要 GRANT。

CREATE DATABASE IF NOT EXISTS tsp;
CREATE DATABASE IF NOT EXISTS tsp_test;
