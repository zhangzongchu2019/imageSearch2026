import { useState, useEffect } from 'react';
import { Card, Button, Row, Col, Tag, Typography, Space, Table, Descriptions, Modal, message } from 'antd';
import { ClockCircleOutlined, PlayCircleOutlined, HistoryOutlined, ReloadOutlined } from '@ant-design/icons';
import { bffApi } from '../../../api/bffApi';
import { formatTimestamp } from '../../../utils/formatters';
import type { SchedulerJob, JobHistory } from '../../../api/types';

const { Title, Text } = Typography;

const jobDescriptions: Record<string, { cron: string; desc: string }> = {
  partition_rotation: { cron: '每月1日 00:00', desc: '分区轮转' },
  uri_dedup_cleanup: { cron: '每月1日 01:00', desc: 'URI 去重清理' },
  bitmap_reconciliation: { cron: '每日 03:00', desc: 'Bitmap 对账' },
  evergreen_pool_check: { cron: '每日 06:00', desc: '常青池治理' },
  milvus_compaction: { cron: '每周日 01:00', desc: 'Milvus 压缩' },
};

function JobCard({ job, onRefresh }: { job: SchedulerJob; onRefresh: () => void }) {
  const [triggering, setTriggering] = useState(false);
  const [historyVisible, setHistoryVisible] = useState(false);
  const [history, setHistory] = useState<JobHistory[]>([]);
  const [loadingHistory, setLoadingHistory] = useState(false);

  const meta = jobDescriptions[job.name] || { cron: job.cron, desc: job.description };

  const handleTrigger = async () => {
    setTriggering(true);
    try {
      await bffApi.triggerJob(job.name);
      message.success(`${job.name} 已触发执行`);
      onRefresh();
    } catch (e: any) {
      message.error('触发失败: ' + e.message);
    } finally {
      setTriggering(false);
    }
  };

  const showHistory = async () => {
    setHistoryVisible(true);
    setLoadingHistory(true);
    try {
      const { data } = await bffApi.getJobHistory(job.name);
      setHistory(data);
    } catch (e: any) {
      message.error('获取历史失败: ' + e.message);
    } finally {
      setLoadingHistory(false);
    }
  };

  const historyColumns = [
    { title: '时间', dataIndex: 'timestamp', render: (v: string) => formatTimestamp(v) },
    {
      title: '状态', dataIndex: 'status',
      render: (v: string) => <Tag color={v === 'success' ? 'green' : 'red'}>{v}</Tag>,
    },
    { title: '耗时', dataIndex: 'duration_ms', render: (v: number) => `${(v / 1000).toFixed(1)}s` },
    { title: '错误', dataIndex: 'error', ellipsis: true },
  ];

  return (
    <>
      <Card style={{ marginBottom: 16 }}>
        <Descriptions column={2} size="small">
          <Descriptions.Item label="任务名">{job.name}</Descriptions.Item>
          <Descriptions.Item label="描述">{meta.desc}</Descriptions.Item>
          <Descriptions.Item label="Cron">{meta.cron}</Descriptions.Item>
          <Descriptions.Item label="下次执行">{job.next_run ? formatTimestamp(job.next_run) : '-'}</Descriptions.Item>
          <Descriptions.Item label="上次执行">{job.last_run ? formatTimestamp(job.last_run) : '-'}</Descriptions.Item>
          <Descriptions.Item label="上次状态">
            {job.last_status ? (
              <Tag color={job.last_status === 'success' ? 'green' : 'red'}>{job.last_status}</Tag>
            ) : '-'}
          </Descriptions.Item>
        </Descriptions>
        <Space style={{ marginTop: 12 }}>
          <Button icon={<PlayCircleOutlined />} type="primary" onClick={handleTrigger} loading={triggering}>
            立即执行
          </Button>
          <Button icon={<HistoryOutlined />} onClick={showHistory}>
            执行历史
          </Button>
        </Space>
      </Card>

      <Modal
        title={`${job.name} 执行历史`}
        open={historyVisible}
        onCancel={() => setHistoryVisible(false)}
        footer={null}
        width={700}
      >
        <Table
          columns={historyColumns}
          dataSource={history}
          rowKey="timestamp"
          size="small"
          loading={loadingHistory}
          pagination={{ pageSize: 10 }}
        />
      </Modal>
    </>
  );
}

export default function Scheduler() {
  const [jobs, setJobs] = useState<SchedulerJob[]>([]);
  const [loading, setLoading] = useState(false);

  const fetchJobs = async () => {
    setLoading(true);
    try {
      const { data } = await bffApi.getJobs();
      setJobs(data);
    } catch (e: any) {
      // If cron-scheduler not available, show placeholder jobs
      setJobs(Object.entries(jobDescriptions).map(([name, meta]) => ({
        name,
        cron: meta.cron,
        description: meta.desc,
        next_run: '',
        last_run: undefined,
        last_status: undefined,
      })));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => { fetchJobs(); }, []);

  return (
    <div>
      <Space style={{ marginBottom: 16, display: 'flex', justifyContent: 'space-between', width: '100%' }}>
        <Title level={4} style={{ margin: 0 }}><ClockCircleOutlined /> 任务调度</Title>
        <Button icon={<ReloadOutlined />} onClick={fetchJobs} loading={loading}>刷新</Button>
      </Space>

      {jobs.map((job) => (
        <JobCard key={job.name} job={job} onRefresh={fetchJobs} />
      ))}
    </div>
  );
}
