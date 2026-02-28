import { useState, useEffect, useRef } from 'react';
import { Card, Button, Row, Col, Tag, Space, Typography, Statistic, message } from 'antd';
import { PlayCircleOutlined, ExperimentOutlined, CheckCircleOutlined, CloseCircleOutlined } from '@ant-design/icons';
import { bffApi } from '../../../api/bffApi';

const { Title, Text, Paragraph } = Typography;

interface TestResult {
  status: 'idle' | 'running' | 'completed' | 'failed';
  output: string;
  passed: number;
  failed: number;
  errors: number;
  runId?: string;
}

const services = [
  { key: 'search-service', label: 'Search Service', lang: 'Python' },
  { key: 'write-service', label: 'Write Service', lang: 'Python' },
  { key: 'cron-scheduler', label: 'Cron Scheduler', lang: 'Python' },
  { key: 'flink-pipeline', label: 'Flink Pipeline', lang: 'Java' },
  { key: 'bitmap-filter-service', label: 'Bitmap Filter', lang: 'Java' },
];

function TestCard({ service, label, lang }: { service: string; label: string; lang: string }) {
  const [result, setResult] = useState<TestResult>({ status: 'idle', output: '', passed: 0, failed: 0, errors: 0 });
  const outputRef = useRef<HTMLPreElement>(null);

  const runTest = async (type: 'unit' | 'integration') => {
    setResult({ status: 'running', output: '', passed: 0, failed: 0, errors: 0 });
    try {
      const { data } = await bffApi.runTests(service, type);
      const runId = data.runId;
      setResult((prev) => ({ ...prev, runId }));

      // Stream output via SSE
      const eventSource = new EventSource(`/api/bff/tests/stream/${runId}`);
      eventSource.onmessage = (e) => {
        const msg = JSON.parse(e.data);
        if (msg.type === 'output') {
          setResult((prev) => {
            const newOutput = prev.output + msg.data;
            return { ...prev, output: newOutput };
          });
          if (outputRef.current) {
            outputRef.current.scrollTop = outputRef.current.scrollHeight;
          }
        } else if (msg.type === 'complete') {
          setResult((prev) => ({
            ...prev,
            status: msg.status,
            passed: msg.passed,
            failed: msg.failed,
            errors: msg.errors,
          }));
          eventSource.close();
        }
      };
      eventSource.onerror = () => {
        setResult((prev) => ({ ...prev, status: 'failed' }));
        eventSource.close();
      };
    } catch (e: any) {
      message.error('启动测试失败: ' + e.message);
      setResult((prev) => ({ ...prev, status: 'failed' }));
    }
  };

  const statusColor = result.status === 'completed' ? 'green' : result.status === 'failed' ? 'red' : result.status === 'running' ? 'blue' : 'default';

  return (
    <Card
      title={
        <Space>
          <ExperimentOutlined />
          <span>{label}</span>
          <Tag>{lang}</Tag>
          <Tag color={statusColor}>{result.status}</Tag>
        </Space>
      }
      style={{ marginBottom: 16 }}
    >
      <Space style={{ marginBottom: 12 }}>
        <Button
          icon={<PlayCircleOutlined />}
          onClick={() => runTest('unit')}
          loading={result.status === 'running'}
          disabled={result.status === 'running'}
        >
          单元测试
        </Button>
        <Button
          icon={<PlayCircleOutlined />}
          onClick={() => runTest('integration')}
          loading={result.status === 'running'}
          disabled={result.status === 'running'}
        >
          集成测试
        </Button>
      </Space>

      {(result.status === 'completed' || result.status === 'failed') && (
        <Row gutter={16} style={{ marginBottom: 12 }}>
          <Col span={8}>
            <Statistic title="通过" value={result.passed} prefix={<CheckCircleOutlined style={{ color: 'green' }} />} />
          </Col>
          <Col span={8}>
            <Statistic title="失败" value={result.failed} prefix={<CloseCircleOutlined style={{ color: 'red' }} />} />
          </Col>
          <Col span={8}>
            <Statistic title="错误" value={result.errors} valueStyle={{ color: result.errors > 0 ? '#cf1322' : undefined }} />
          </Col>
        </Row>
      )}

      {result.output && (
        <pre
          ref={outputRef}
          style={{
            background: '#1e1e1e', color: '#d4d4d4', padding: 12, borderRadius: 4,
            maxHeight: 300, overflow: 'auto', fontSize: 12, whiteSpace: 'pre-wrap',
          }}
        >
          {result.output}
        </pre>
      )}
    </Card>
  );
}

export default function Tests() {
  const [runningAll, setRunningAll] = useState(false);

  const runAll = async () => {
    setRunningAll(true);
    try {
      await bffApi.runTests('all', 'unit');
      message.info('全部测试已启动');
    } catch (e: any) {
      message.error('启动失败: ' + e.message);
    } finally {
      setRunningAll(false);
    }
  };

  return (
    <div>
      <Space style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', width: '100%' }}>
        <Title level={4} style={{ margin: 0 }}>测试运行</Title>
        <Button type="primary" icon={<PlayCircleOutlined />} onClick={runAll} loading={runningAll}>
          运行全部
        </Button>
      </Space>

      {services.map((s) => (
        <TestCard key={s.key} service={s.key} label={s.label} lang={s.lang} />
      ))}
    </div>
  );
}
