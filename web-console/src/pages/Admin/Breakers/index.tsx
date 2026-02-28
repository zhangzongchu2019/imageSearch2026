import { useState } from 'react';
import { Card, Table, Tag, Button, Modal, Select, message, Typography, Space } from 'antd';
import { ReloadOutlined, ThunderboltOutlined } from '@ant-design/icons';
import { adminApi } from '../../../api/adminApi';
import { useSystemStore } from '../../../stores/systemStore';
import { usePolling } from '../../../hooks/usePolling';
import { formatTimestamp } from '../../../utils/formatters';
import type { BreakerInfo, BreakerState } from '../../../api/types';

const { Title } = Typography;

const stateColors: Record<BreakerState, string> = {
  closed: 'green',
  half_open: 'orange',
  open: 'red',
};

export default function Breakers() {
  const { breakers, fetchBreakers } = useSystemStore();
  const [forceTarget, setForceTarget] = useState<BreakerInfo | null>(null);
  const [targetState, setTargetState] = useState<BreakerState>('closed');
  const [loading, setLoading] = useState(false);

  usePolling(fetchBreakers, 5000);

  const handleForce = async () => {
    if (!forceTarget) return;
    setLoading(true);
    try {
      await adminApi.forceBreaker(forceTarget.name, targetState);
      message.success(`${forceTarget.name} 已设置为 ${targetState}`);
      setForceTarget(null);
      fetchBreakers();
    } catch (e: any) {
      message.error('操作失败: ' + e.message);
    } finally {
      setLoading(false);
    }
  };

  const columns = [
    { title: '名称', dataIndex: 'name', key: 'name' },
    {
      title: '状态',
      dataIndex: 'state',
      key: 'state',
      render: (state: BreakerState) => <Tag color={stateColors[state]}>{state}</Tag>,
    },
    { title: '失败计数', dataIndex: 'fail_count', key: 'fail_count' },
    {
      title: '上次变更',
      dataIndex: 'last_state_change',
      key: 'last_state_change',
      render: (v: string) => v ? formatTimestamp(v) : '-',
    },
    {
      title: '操作',
      key: 'action',
      render: (_: unknown, record: BreakerInfo) => (
        <Button type="link" icon={<ThunderboltOutlined />} onClick={() => { setForceTarget(record); setTargetState('closed'); }}>
          强制状态
        </Button>
      ),
    },
  ];

  return (
    <div>
      <Space style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', width: '100%' }}>
        <Title level={4} style={{ margin: 0 }}>熔断器管理</Title>
        <Button icon={<ReloadOutlined />} onClick={fetchBreakers}>刷新</Button>
      </Space>

      <Table columns={columns} dataSource={breakers} rowKey="name" pagination={false} />

      <Modal
        title={`强制设置: ${forceTarget?.name}`}
        open={!!forceTarget}
        onOk={handleForce}
        onCancel={() => setForceTarget(null)}
        confirmLoading={loading}
      >
        <Select
          value={targetState}
          onChange={setTargetState}
          style={{ width: '100%' }}
          options={[
            { label: 'closed (关闭 - 正常放行)', value: 'closed' },
            { label: 'open (打开 - 全部熔断)', value: 'open' },
            { label: 'half_open (半开 - 探测恢复)', value: 'half_open' },
          ]}
        />
      </Modal>
    </div>
  );
}
