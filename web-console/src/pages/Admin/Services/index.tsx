import { useState, useEffect, useRef, useCallback } from 'react';
import { Card, Row, Col, Tag, Button, Space, Modal, message, Spin } from 'antd';
import {
  ReloadOutlined,
  PlayCircleOutlined,
  PauseCircleOutlined,
  RedoOutlined,
  FileTextOutlined,
  LinkOutlined,
} from '@ant-design/icons';
import { bffApi } from '../../../api/bffApi';
import type { ServiceInfo } from '../../../api/types';

// 服务管理界面 URL 映射（基于 docker-compose 端口映射）
const SERVICE_UI_URLS: Record<string, { url: string; label: string }> = {
  'clickhouse':        { url: 'http://localhost:8123/play', label: 'ClickHouse Play' },
  'grafana':           { url: 'http://localhost:3002',      label: 'Grafana' },
  'prometheus':        { url: 'http://localhost:9099',      label: 'Prometheus' },
  'flink-jobmanager':  { url: 'http://localhost:8083',      label: 'Flink Dashboard' },
  'milvus-standalone': { url: 'http://localhost:9091/webui', label: 'Milvus WebUI' },
  'search-service':    { url: 'http://localhost:8080/docs',  label: 'Search API Docs' },
  'write-service':     { url: 'http://localhost:8081/docs',  label: 'Write API Docs' },
  'cron-scheduler':    { url: 'http://localhost:8082/docs',  label: 'Cron API Docs' },
  'minio':             { url: 'http://localhost:9001',       label: 'MinIO Console' },
};

const stateColor = (state: string) => {
  if (state === 'running') return 'green';
  if (state === 'exited') return 'red';
  return 'default';
};

const stateLabel = (state: string) => {
  if (state === 'running') return '运行中';
  if (state === 'exited') return '已停止';
  if (state === 'not_found') return '未启动';
  if (state === 'restarting') return '重启中';
  return state;
};

export default function Services() {
  const [services, setServices] = useState<ServiceInfo[]>([]);
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState<string | null>(null);
  const [logTarget, setLogTarget] = useState<string | null>(null);
  const [logText, setLogText] = useState('');
  const [streamId, setStreamId] = useState<string | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const logEndRef = useRef<HTMLDivElement>(null);

  const fetchServices = useCallback(async () => {
    setLoading(true);
    try {
      const { data } = await bffApi.getServices();
      setServices(data);
    } catch (e: any) {
      message.error('获取服务状态失败: ' + e.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchServices();
    const timer = setInterval(fetchServices, 5000);
    return () => clearInterval(timer);
  }, [fetchServices]);

  useEffect(() => {
    logEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [logText]);

  const handleAction = async (name: string, action: 'start' | 'stop' | 'restart') => {
    Modal.confirm({
      title: `确认${action === 'start' ? '启动' : action === 'stop' ? '停止' : '重启'} ${name}？`,
      onOk: async () => {
        setActionLoading(`${name}-${action}`);
        try {
          await bffApi.controlService(name, action);
          message.success(`${name} ${action} 成功`);
          fetchServices();
        } catch (e: any) {
          message.error(`操作失败: ${e.response?.data?.error || e.message}`);
        } finally {
          setActionLoading(null);
        }
      },
    });
  };

  const openLogs = (name: string) => {
    setLogTarget(name);
    setLogText('');

    const es = new EventSource(`/api/bff/services/${name}/logs?tail=200`);
    eventSourceRef.current = es;

    es.onmessage = (event) => {
      try {
        const msg = JSON.parse(event.data);
        if (msg.type === 'init') {
          setStreamId(msg.streamId);
        } else if (msg.type === 'log') {
          setLogText((prev) => prev + msg.text);
        } else if (msg.type === 'end') {
          es.close();
        }
      } catch {
        // ignore parse errors
      }
    };

    es.onerror = () => {
      es.close();
    };
  };

  const closeLogs = () => {
    eventSourceRef.current?.close();
    eventSourceRef.current = null;
    if (logTarget && streamId) {
      bffApi.stopLogStream(logTarget, streamId).catch(() => {});
    }
    setLogTarget(null);
    setLogText('');
    setStreamId(null);
  };

  return (
    <Card
      title="服务管理"
      extra={
        <Button icon={<ReloadOutlined />} onClick={fetchServices} loading={loading}>
          刷新
        </Button>
      }
    >
      <Spin spinning={loading && services.length === 0}>
        <Row gutter={[16, 16]}>
          {services.map((svc) => {
            const uiLink = SERVICE_UI_URLS[svc.name];
            return (
              <Col key={svc.name} xs={24} sm={12} md={8} lg={6}>
                <Card size="small" title={svc.name} extra={<Tag color={stateColor(svc.state)}>{stateLabel(svc.state)}</Tag>}>
                  {svc.ports && (
                    <div style={{ fontSize: 12, color: '#888', marginBottom: 8, wordBreak: 'break-all' }}>
                      {svc.ports}
                    </div>
                  )}
                  {uiLink && (
                    <div style={{ marginBottom: 8 }}>
                      <a href={uiLink.url} target="_blank" rel="noopener noreferrer">
                        <Button size="small" type="link" icon={<LinkOutlined />} style={{ padding: 0 }}>
                          {uiLink.label}
                        </Button>
                      </a>
                    </div>
                  )}
                  <Space wrap>
                    <Button
                      size="small"
                      icon={<PlayCircleOutlined />}
                      loading={actionLoading === `${svc.name}-start`}
                      onClick={() => handleAction(svc.name, 'start')}
                    >
                      启动
                    </Button>
                    <Button
                      size="small"
                      icon={<PauseCircleOutlined />}
                      loading={actionLoading === `${svc.name}-stop`}
                      onClick={() => handleAction(svc.name, 'stop')}
                    >
                      停止
                    </Button>
                    <Button
                      size="small"
                      icon={<RedoOutlined />}
                      loading={actionLoading === `${svc.name}-restart`}
                      onClick={() => handleAction(svc.name, 'restart')}
                    >
                      重启
                    </Button>
                    <Button
                      size="small"
                      icon={<FileTextOutlined />}
                      onClick={() => openLogs(svc.name)}
                    >
                      日志
                    </Button>
                  </Space>
                </Card>
              </Col>
            );
          })}
        </Row>
      </Spin>

      <Modal
        open={!!logTarget}
        title={`日志 - ${logTarget}`}
        onCancel={closeLogs}
        width={900}
        footer={
          <Button onClick={closeLogs}>关闭日志</Button>
        }
      >
        <pre
          style={{
            background: '#1e1e1e',
            color: '#d4d4d4',
            padding: 16,
            borderRadius: 8,
            maxHeight: 500,
            overflow: 'auto',
            fontSize: 12,
            whiteSpace: 'pre-wrap',
            wordBreak: 'break-all',
          }}
        >
          {logText || '加载中...'}
          <div ref={logEndRef} />
        </pre>
      </Modal>
    </Card>
  );
}
