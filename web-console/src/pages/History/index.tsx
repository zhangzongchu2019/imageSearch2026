import { useState } from 'react';
import { Card, Tabs, Table, Tag, Button, Modal, Typography, Popconfirm } from 'antd';
import { DeleteOutlined } from '@ant-design/icons';
import { useHistory } from '../../hooks/useHistory';
import { formatTimestamp, truncate } from '../../utils/formatters';
import type { HistoryRecord } from '../../utils/db';

const { Text } = Typography;

export default function History() {
  const { imports, searches, loading, clearAll } = useHistory();
  const [detail, setDetail] = useState<HistoryRecord | null>(null);

  const columns = [
    {
      title: '时间',
      dataIndex: 'timestamp',
      width: 180,
      render: (v: number) => formatTimestamp(v),
    },
    {
      title: '参数',
      dataIndex: 'params',
      ellipsis: true,
      render: (v: Record<string, unknown>) => <Text style={{ fontSize: 12 }}>{truncate(JSON.stringify(v), 80)}</Text>,
    },
    {
      title: '结果',
      dataIndex: 'result',
      ellipsis: true,
      render: (v: Record<string, unknown>) => <Text style={{ fontSize: 12 }}>{truncate(JSON.stringify(v), 60)}</Text>,
    },
    {
      title: '操作',
      width: 80,
      render: (_: unknown, record: HistoryRecord) => (
        <Button type="link" size="small" onClick={() => setDetail(record)}>详情</Button>
      ),
    },
  ];

  return (
    <Card
      title="历史记录"
      extra={
        <Popconfirm title="确定清空所有历史记录?" onConfirm={clearAll}>
          <Button danger icon={<DeleteOutlined />} size="small">清空</Button>
        </Popconfirm>
      }
    >
      <Tabs items={[
        {
          key: 'import',
          label: `导入历史 (${imports.length})`,
          children: <Table columns={columns} dataSource={imports} rowKey="id" size="small" loading={loading} pagination={{ pageSize: 20 }} />,
        },
        {
          key: 'search',
          label: `查询历史 (${searches.length})`,
          children: <Table columns={columns} dataSource={searches} rowKey="id" size="small" loading={loading} pagination={{ pageSize: 20 }} />,
        },
      ]} />

      <Modal
        open={!!detail}
        onCancel={() => setDetail(null)}
        title="详细信息"
        footer={null}
        width={700}
      >
        {detail && (
          <div>
            <Card title="请求参数" size="small" style={{ marginBottom: 12 }}>
              <pre style={{ fontSize: 12, maxHeight: 300, overflow: 'auto' }}>
                {JSON.stringify(detail.params, null, 2)}
              </pre>
            </Card>
            <Card title="响应结果" size="small">
              <pre style={{ fontSize: 12, maxHeight: 400, overflow: 'auto' }}>
                {JSON.stringify(detail.result, null, 2)}
              </pre>
            </Card>
          </div>
        )}
      </Modal>
    </Card>
  );
}
