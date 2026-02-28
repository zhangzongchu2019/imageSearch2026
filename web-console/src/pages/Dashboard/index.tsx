import { useState } from 'react';
import { Card, Row, Col, Statistic, Tag, Typography, Descriptions, Progress, Alert } from 'antd';
import {
  CheckCircleOutlined, CloseCircleOutlined, DisconnectOutlined,
  ThunderboltOutlined, DashboardOutlined, LoadingOutlined,
} from '@ant-design/icons';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from 'recharts';
import { usePolling } from '../../hooks/usePolling';
import { useSystemStore } from '../../stores/systemStore';
import type { DegradeState, BreakerState } from '../../api/types';

const { Title, Text } = Typography;

const degradeColors: Record<DegradeState, string> = {
  S0: 'green', S1: 'blue', S2: 'orange', S3: 'volcano', S4: 'red',
};

const degradeLabels: Record<DegradeState, string> = {
  S0: '正常', S1: '轻度降级', S2: '中度降级', S3: '恢复中', S4: '全面降级',
};

const breakerColors: Record<BreakerState, string> = {
  closed: 'green', half_open: 'orange', open: 'red',
};

// FSM state machine visualization
function FsmDiagram({ current }: { current: DegradeState }) {
  const states: DegradeState[] = ['S0', 'S1', 'S2', 'S3', 'S4'];
  return (
    <div style={{ display: 'flex', alignItems: 'center', gap: 8, flexWrap: 'wrap' }}>
      {states.map((s, i) => (
        <div key={s} style={{ display: 'flex', alignItems: 'center' }}>
          <div style={{
            width: 60, height: 60, borderRadius: '50%',
            border: `3px solid ${s === current ? '#1677ff' : '#d9d9d9'}`,
            background: s === current ? '#e6f4ff' : '#fafafa',
            display: 'flex', flexDirection: 'column',
            alignItems: 'center', justifyContent: 'center',
            fontWeight: s === current ? 700 : 400,
          }}>
            <div style={{ fontSize: 16 }}>{s}</div>
            <div style={{ fontSize: 9, color: '#666' }}>{degradeLabels[s]}</div>
          </div>
          {i < states.length - 1 && (
            <div style={{ width: 30, height: 2, background: '#d9d9d9', margin: '0 4px' }} />
          )}
        </div>
      ))}
    </div>
  );
}

export default function Dashboard() {
  const { status, degradeStatus, breakers, rateLimiter, connected, loading, fetchAll } = useSystemStore();
  const [metrics, setMetrics] = useState<Array<{ time: string; p99: number; qps: number; errors: number }>>([]);

  usePolling(() => {
    fetchAll();
    if (connected) {
      setMetrics((prev) => {
        const now = new Date().toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit', second: '2-digit' });
        const next = [...prev, {
          time: now,
          p99: degradeStatus?.window_metrics?.p99_ms ?? 0,
          qps: Math.random() * 100,
          errors: degradeStatus?.window_metrics?.error_rate ?? 0,
        }];
        return next.slice(-60);
      });
    }
  }, 5000);

  const isHealthy = status?.status === 'ok' || status?.status === 'healthy';

  return (
    <div>
      <Title level={4}><DashboardOutlined /> Dashboard</Title>

      {/* Disconnected Banner */}
      {!connected && (
        <Alert
          message="后端服务未连接"
          description="search-service (端口 8080) 暂未运行。请确保后端服务已启动后，数据将自动刷新。"
          type="warning"
          showIcon
          icon={<DisconnectOutlined />}
          style={{ marginBottom: 24 }}
        />
      )}

      {/* Health Cards */}
      <Row gutter={[16, 16]} style={{ marginBottom: 24 }}>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="系统状态"
              value={!connected ? '未连接' : isHealthy ? '健康' : '异常'}
              valueStyle={{ color: !connected ? '#8c8c8c' : isHealthy ? '#3f8600' : '#cf1322' }}
              prefix={!connected ? <DisconnectOutlined /> : isHealthy ? <CheckCircleOutlined /> : <CloseCircleOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="降级状态"
              value={!connected ? '-' : degradeStatus?.current_state || '-'}
              valueStyle={{ color: !connected ? '#8c8c8c' : degradeColors[degradeStatus?.current_state || 'S0'] === 'green' ? '#3f8600' : '#cf1322' }}
              prefix={<ThunderboltOutlined />}
            />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic title="P99 延迟" value={!connected ? '-' : degradeStatus?.window_metrics?.p99_ms ?? '-'} suffix={connected ? 'ms' : ''} />
          </Card>
        </Col>
        <Col xs={12} sm={6}>
          <Card>
            <Statistic
              title="错误率"
              value={!connected ? '-' : ((degradeStatus?.window_metrics?.error_rate ?? 0) * 100).toFixed(2)}
              suffix={connected ? '%' : ''}
              valueStyle={{ color: !connected ? '#8c8c8c' : (degradeStatus?.window_metrics?.error_rate ?? 0) > 0.01 ? '#cf1322' : '#3f8600' }}
            />
          </Card>
        </Col>
      </Row>

      {/* FSM Visualization */}
      <Card title="降级 FSM 状态" style={{ marginBottom: 24 }}>
        {connected ? (
          <>
            <FsmDiagram current={degradeStatus?.current_state || 'S0'} />
            {degradeStatus?.manual_override && (
              <Tag color="orange" style={{ marginTop: 12 }}>手动锁定: {degradeStatus.override_reason}</Tag>
            )}
          </>
        ) : (
          <Text type="secondary">等待后端服务连接...</Text>
        )}
      </Card>

      {/* Circuit Breaker Grid */}
      <Card title="熔断器状态" style={{ marginBottom: 24 }}>
        <Row gutter={[12, 12]}>
          {breakers.map((b) => (
            <Col key={b.name} xs={12} sm={8} md={6}>
              <Card size="small">
                <Text strong style={{ fontSize: 12 }}>{b.name}</Text>
                <div style={{ marginTop: 4 }}>
                  <Tag color={breakerColors[b.state]}>{b.state}</Tag>
                </div>
                <Text type="secondary" style={{ fontSize: 11 }}>失败: {b.fail_count}</Text>
              </Card>
            </Col>
          ))}
          {breakers.length === 0 && (
            <Col><Text type="secondary">{connected ? '暂无熔断器数据' : '等待后端服务连接...'}</Text></Col>
          )}
        </Row>
      </Card>

      {/* Metrics Charts */}
      <Row gutter={16} style={{ marginBottom: 24 }}>
        <Col span={12}>
          <Card title="P99 延迟 (ms)" size="small">
            {metrics.length > 0 ? (
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={metrics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time" tick={{ fontSize: 10 }} />
                  <YAxis />
                  <Tooltip />
                  <Line type="monotone" dataKey="p99" stroke="#1677ff" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Text type="secondary">等待数据...</Text>
              </div>
            )}
          </Card>
        </Col>
        <Col span={12}>
          <Card title="错误率 (%)" size="small">
            {metrics.length > 0 ? (
              <ResponsiveContainer width="100%" height={200}>
                <LineChart data={metrics}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis dataKey="time" tick={{ fontSize: 10 }} />
                  <YAxis />
                  <Tooltip />
                  <Line type="monotone" dataKey="errors" stroke="#cf1322" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            ) : (
              <div style={{ height: 200, display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                <Text type="secondary">等待数据...</Text>
              </div>
            )}
          </Card>
        </Col>
      </Row>

      {/* Rate Limiter */}
      {rateLimiter && (
        <Card title="令牌桶状态" size="small">
          <Progress
            percent={Math.round((rateLimiter.tokens_remaining / rateLimiter.max_tokens) * 100)}
            format={() => `${rateLimiter.tokens_remaining} / ${rateLimiter.max_tokens}`}
          />
          <Text type="secondary" style={{ fontSize: 12 }}>补充速率: {rateLimiter.refill_rate}/s</Text>
        </Card>
      )}
    </div>
  );
}
