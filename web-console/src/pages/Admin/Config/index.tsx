import { useState, useEffect } from 'react';
import { Card, Button, Table, Tag, message, Typography, Space } from 'antd';
import { ReloadOutlined, FileTextOutlined } from '@ant-design/icons';
import { adminApi } from '../../../api/adminApi';
import { formatTimestamp } from '../../../utils/formatters';
import type { ConfigAuditEntry } from '../../../api/types';

const { Title, Text } = Typography;

export default function Config() {
  const [auditLog, setAuditLog] = useState<ConfigAuditEntry[]>([]);
  const [loading, setLoading] = useState(false);
  const [reloading, setReloading] = useState(false);

  const fetchAudit = async () => {
    setLoading(true);
    try {
      const { data } = await adminApi.getConfigAudit();
      setAuditLog(data);
    } catch (e: any) {
      message.error('获取审计日志失败: ' + e.message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchAudit(); }, []);

  const handleReload = async () => {
    setReloading(true);
    try {
      await adminApi.reloadConfig();
      message.success('配置已重新加载');
      fetchAudit();
    } catch (e: any) {
      message.error('重新加载失败: ' + e.message);
    } finally {
      setReloading(false);
    }
  };

  const columns = [
    { title: '版本', dataIndex: 'version', width: 80 },
    { title: '时间', dataIndex: 'timestamp', width: 180, render: (v: string) => formatTimestamp(v) },
    { title: '配置键', dataIndex: 'key', ellipsis: true },
    {
      title: '变更',
      key: 'change',
      render: (_: unknown, record: ConfigAuditEntry) => (
        <span>
          <Text delete type="danger" style={{ fontSize: 12 }}>{record.old_value}</Text>
          {' → '}
          <Text type="success" style={{ fontSize: 12 }}>{record.new_value}</Text>
        </span>
      ),
    },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', width: '100%' }}>
        <Title level={4} style={{ margin: 0 }}>配置管理</Title>
        <Space>
          <Button icon={<ReloadOutlined />} type="primary" onClick={handleReload} loading={reloading}>
            重新加载配置
          </Button>
          <Button icon={<FileTextOutlined />} onClick={fetchAudit}>
            刷新日志
          </Button>
        </Space>
      </Space>

      <Card title="审计日志">
        <Table
          columns={columns}
          dataSource={auditLog}
          rowKey={(r) => `${r.version}-${r.key}`}
          size="small"
          loading={loading}
          pagination={{ pageSize: 20 }}
        />
      </Card>
    </div>
  );
}
