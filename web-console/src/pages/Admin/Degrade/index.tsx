import { useState, useEffect } from 'react';
import { Card, Button, Tag, Descriptions, Modal, Select, Input, message, Steps, Progress, Space, Typography } from 'antd';
import { LockOutlined, UnlockOutlined, ReloadOutlined } from '@ant-design/icons';
import { adminApi } from '../../../api/adminApi';
import { useSystemStore } from '../../../stores/systemStore';
import { usePolling } from '../../../hooks/usePolling';
import type { DegradeState } from '../../../api/types';

const { Text, Title } = Typography;

const stateDescriptions: Record<DegradeState, string> = {
  S0: '正常状态 - 全功能可用',
  S1: '轻度降级 - 缩小搜索范围 (仅热区+常青)',
  S2: '中度降级 - 禁用级联、缩减参数',
  S3: '恢复阶段 - 渐进式恢复 (ramp 0.1→0.5→1.0)',
  S4: '全面降级 - 返回空结果兜底',
};

const stateColors: Record<DegradeState, string> = {
  S0: 'green', S1: 'blue', S2: 'orange', S3: 'cyan', S4: 'red',
};

export default function Degrade() {
  const { degradeStatus, fetchDegradeStatus } = useSystemStore();
  const [overrideVisible, setOverrideVisible] = useState(false);
  const [selectedState, setSelectedState] = useState<DegradeState>('S1');
  const [reason, setReason] = useState('');
  const [loading, setLoading] = useState(false);

  usePolling(fetchDegradeStatus, 5000);

  const handleOverride = async () => {
    if (!reason.trim()) {
      message.warning('请填写原因');
      return;
    }
    setLoading(true);
    try {
      await adminApi.overrideDegrade(selectedState, reason);
      message.success(`已锁定到 ${selectedState}`);
      setOverrideVisible(false);
      setReason('');
      fetchDegradeStatus();
    } catch (e: any) {
      message.error('操作失败: ' + e.message);
    } finally {
      setLoading(false);
    }
  };

  const handleRelease = async () => {
    setLoading(true);
    try {
      await adminApi.releaseDegrade();
      message.success('已解除手动锁定');
      fetchDegradeStatus();
    } catch (e: any) {
      message.error('操作失败: ' + e.message);
    } finally {
      setLoading(false);
    }
  };

  const currentState = degradeStatus?.current_state || 'S0';
  const stateIndex = ['S0', 'S1', 'S2', 'S3', 'S4'].indexOf(currentState);

  return (
    <div>
      <Title level={4}>降级管理</Title>

      {/* Current State Card */}
      <Card style={{ marginBottom: 24 }}>
        <Descriptions column={2}>
          <Descriptions.Item label="当前状态">
            <Tag color={stateColors[currentState]} style={{ fontSize: 16, padding: '4px 12px' }}>
              {currentState}
            </Tag>
            <Text style={{ marginLeft: 8 }}>{stateDescriptions[currentState]}</Text>
          </Descriptions.Item>
          <Descriptions.Item label="Epoch">{degradeStatus?.epoch ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="P99 延迟">{degradeStatus?.window_metrics?.p99_ms ?? '-'} ms</Descriptions.Item>
          <Descriptions.Item label="错误率">{((degradeStatus?.window_metrics?.error_rate ?? 0) * 100).toFixed(2)}%</Descriptions.Item>
          <Descriptions.Item label="超时次数">{degradeStatus?.window_metrics?.timeout_count ?? '-'}</Descriptions.Item>
          <Descriptions.Item label="Redis 连接">{degradeStatus?.redis_connected ? <Tag color="green">已连接</Tag> : <Tag color="red">断开</Tag>}</Descriptions.Item>
          <Descriptions.Item label="手动锁定">
            {degradeStatus?.manual_override ? (
              <Tag color="orange">是 - {degradeStatus.override_reason}</Tag>
            ) : (
              <Tag color="green">否</Tag>
            )}
          </Descriptions.Item>
        </Descriptions>

        <Space style={{ marginTop: 16 }}>
          <Button type="primary" danger icon={<LockOutlined />} onClick={() => setOverrideVisible(true)}>
            手动锁定
          </Button>
          <Button icon={<UnlockOutlined />} onClick={handleRelease} disabled={!degradeStatus?.manual_override} loading={loading}>
            解除锁定
          </Button>
          <Button icon={<ReloadOutlined />} onClick={fetchDegradeStatus}>
            刷新
          </Button>
        </Space>
      </Card>

      {/* State steps visualization */}
      <Card title="状态进度" style={{ marginBottom: 24 }}>
        <Steps
          current={stateIndex}
          items={(['S0', 'S1', 'S2', 'S3', 'S4'] as DegradeState[]).map((s) => ({
            title: s,
            description: stateDescriptions[s].split(' - ')[1],
            status: s === currentState ? 'process' : stateIndex > ['S0', 'S1', 'S2', 'S3', 'S4'].indexOf(s) ? 'finish' : 'wait',
          }))}
        />
      </Card>

      {/* S3 Recovery progress */}
      {currentState === 'S3' && (
        <Card title="S3 恢复进度">
          <Steps
            current={0}
            items={[
              { title: 'Ramp 0.1', description: '10% 流量' },
              { title: 'Ramp 0.5', description: '50% 流量' },
              { title: 'Ramp 1.0', description: '100% 流量 → S0' },
            ]}
          />
        </Card>
      )}

      {/* Override Modal */}
      <Modal
        title="手动锁定降级状态"
        open={overrideVisible}
        onOk={handleOverride}
        onCancel={() => setOverrideVisible(false)}
        confirmLoading={loading}
      >
        <div style={{ marginBottom: 16 }}>
          <Text>选择目标状态:</Text>
          <Select
            value={selectedState}
            onChange={setSelectedState}
            style={{ width: '100%', marginTop: 8 }}
            options={(['S0', 'S1', 'S2', 'S3', 'S4'] as DegradeState[]).map((s) => ({
              label: `${s} - ${stateDescriptions[s].split(' - ')[1]}`,
              value: s,
            }))}
          />
        </div>
        <div>
          <Text>原因:</Text>
          <Input.TextArea
            value={reason}
            onChange={(e) => setReason(e.target.value)}
            placeholder="例: 计划维护窗口"
            rows={3}
            style={{ marginTop: 8 }}
          />
        </div>
      </Modal>
    </div>
  );
}
