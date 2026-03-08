-- 迁移脚本: 为 uri_dedup 表添加 uri 列 (存储原始图片 URL)
-- 已有数据库执行此脚本; 新建库由 init_db.sql 自动包含
ALTER TABLE uri_dedup ADD COLUMN IF NOT EXISTS uri TEXT;
