import { useState, useRef, useCallback, useEffect } from 'react';
import { Tabs, Card, Upload, Form, Input, InputNumber, Button, Select, Switch, message, Table, Tag, Progress, Alert } from 'antd';
import { InboxOutlined, UploadOutlined, FileTextOutlined, PlayCircleOutlined } from '@ant-design/icons';
import type { UploadFile } from 'antd';
import { useImageUpload } from '../../hooks/useImageUpload';
import { useBatchOperation } from '../../hooks/useBatchOperation';
import { bffApi } from '../../api/bffApi';
import { useHistoryStore } from '../../stores/historyStore';
import { randomMerchantId, randomProductId } from '../../utils/testData';
import type { FileImportProgress } from '../../api/types';

const { Dragger } = Upload;
const { TextArea } = Input;

const categories = [
  { label: '服装', value: '服装' },
  { label: '鞋靴', value: '鞋靴' },
  { label: '经典箱包', value: '经典箱包' },
  { label: '家具', value: '家具' },
  { label: '珠宝', value: '珠宝' },
  { label: '数码', value: '数码' },
];

function SingleImport() {
  const [form] = Form.useForm();
  const { upload, uploading, uploadedUrl } = useImageUpload();
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<any>(null);
  const [testMode, setTestMode] = useState(false);
  const addRecord = useHistoryStore((s) => s.addRecord);

  const handleTestModeChange = (checked: boolean) => {
    setTestMode(checked);
    if (checked) {
      form.setFieldsValue({
        merchant_id: randomMerchantId(),
        product_id: randomProductId(),
        category_l1: '服装',
      });
    } else {
      form.setFieldsValue({ merchant_id: undefined, product_id: undefined, category_l1: undefined });
    }
  };

  const handleUpload = async (file: File) => {
    await upload(file);
    return false; // prevent default upload
  };

  const handleSubmit = async () => {
    if (!uploadedUrl) {
      message.warning('请先上传图片');
      return;
    }
    const values = await form.validateFields();
    setSubmitting(true);
    try {
      const { data } = await bffApi.importImage({
        url: uploadedUrl,
        merchant_id: values.merchant_id,
        category_l1: values.category_l1,
        product_id: values.product_id,
        tags: values.tags ? values.tags.split(',').map((t: string) => t.trim()) : undefined,
      });
      setResult(data);
      message.success(`导入成功: ${data.image_id} (${data.is_new ? '新图' : '已存在'})`);
      addRecord({ type: 'import', params: values, result: data });
    } catch (e: any) {
      message.error('导入失败: ' + (e.response?.data?.message || e.message));
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div style={{ maxWidth: 600 }}>
      <Dragger
        accept="image/*"
        maxCount={1}
        beforeUpload={(file) => { handleUpload(file); return false; }}
        showUploadList
        style={{ marginBottom: 24 }}
      >
        <p className="ant-upload-drag-icon"><InboxOutlined /></p>
        <p className="ant-upload-text">点击或拖拽图片到此区域上传</p>
        <p className="ant-upload-hint">支持 JPG/PNG，最大 10MB</p>
      </Dragger>

      <Form form={form} layout="vertical">
        <Form.Item label="测试模式" tooltip="开启后自动填充测试参数">
          <Switch checked={testMode} onChange={handleTestModeChange} />
        </Form.Item>
        <Form.Item name="merchant_id" label="商家 ID" rules={[{ required: true }]}>
          <Input placeholder="merchant_001" />
        </Form.Item>
        <Form.Item name="category_l1" label="一级分类" rules={[{ required: true }]}>
          <Select options={categories} placeholder="选择分类" />
        </Form.Item>
        <Form.Item name="product_id" label="商品 ID" rules={[{ required: true }]}>
          <Input placeholder="prod_12345" />
        </Form.Item>
        <Form.Item name="tags" label="标签 (逗号分隔)">
          <Input placeholder="红色, 连衣裙, 夏季" />
        </Form.Item>
        <Button type="primary" icon={<UploadOutlined />} loading={uploading || submitting} onClick={handleSubmit}>
          导入图片
        </Button>
      </Form>

      {result && (
        <Alert
          style={{ marginTop: 16 }}
          type="success"
          message={`图片 ID: ${result.image_id}`}
          description={result.is_new ? '新图片已入库' : '图片已存在，已更新元数据'}
          showIcon
        />
      )}
    </div>
  );
}

function BatchImport() {
  const [form] = Form.useForm();
  const [urlList, setUrlList] = useState('');
  const [testMode, setTestMode] = useState(false);
  const { progress, running, start } = useBatchOperation('/api/bff/batch/import');

  const handleTestModeChange = (checked: boolean) => {
    setTestMode(checked);
    if (checked) {
      form.setFieldsValue({
        merchant_id: randomMerchantId(),
        product_id: randomProductId(),
        category_l1: '服装',
      });
    } else {
      form.setFieldsValue({ merchant_id: undefined, product_id: undefined, category_l1: undefined });
    }
  };

  const handleStart = async () => {
    const meta = await form.validateFields();
    const formData = new FormData();
    formData.append('metadata', JSON.stringify({
      ...meta,
      tags: meta.tags ? meta.tags.split(',').map((t: string) => t.trim()) : undefined,
    }));
    if (urlList.trim()) {
      formData.append('urls', JSON.stringify(urlList.split('\n').filter(Boolean)));
    }
    start(formData);
  };

  const columns = [
    { title: '#', dataIndex: 'index', width: 60 },
    { title: '状态', dataIndex: 'success', width: 80, render: (v: boolean) => v ? <Tag color="green">成功</Tag> : <Tag color="red">失败</Tag> },
    { title: '图片 ID', dataIndex: 'image_id' },
    { title: '新图', dataIndex: 'is_new', width: 80, render: (v: boolean) => v ? '是' : '否' },
    { title: '错误', dataIndex: 'error', ellipsis: true },
  ];

  return (
    <div>
      <Form form={form} layout="vertical" style={{ maxWidth: 600 }}>
        <Form.Item label="测试模式" tooltip="开启后自动填充测试参数">
          <Switch checked={testMode} onChange={handleTestModeChange} />
        </Form.Item>
        <Form.Item name="merchant_id" label="商家 ID" rules={[{ required: true }]}>
          <Input />
        </Form.Item>
        <Form.Item name="category_l1" label="一级分类" rules={[{ required: true }]}>
          <Select options={categories.map((c) => ({ label: c, value: c }))} />
        </Form.Item>
        <Form.Item name="product_id" label="商品 ID" rules={[{ required: true }]}>
          <Input />
        </Form.Item>
        <Form.Item name="tags" label="标签 (逗号分隔)">
          <Input />
        </Form.Item>
      </Form>

      <Card title="URL 列表 (每行一个)" size="small" style={{ marginBottom: 16, maxWidth: 600 }}>
        <TextArea rows={6} value={urlList} onChange={(e) => setUrlList(e.target.value)} placeholder="https://example.com/img1.jpg&#10;https://example.com/img2.jpg" />
      </Card>

      <Button type="primary" loading={running} onClick={handleStart} disabled={running}>
        {running ? '导入中...' : '开始批量导入'}
      </Button>

      {progress && (
        <div style={{ marginTop: 16 }}>
          <Progress
            percent={progress.total ? Math.round((progress.completed / progress.total) * 100) : 0}
            status={running ? 'active' : undefined}
            format={() => `${progress.completed}/${progress.total} (成功 ${progress.succeeded}, 失败 ${progress.failed})`}
          />
          {progress.results.length > 0 && (
            <Table
              columns={columns}
              dataSource={progress.results}
              rowKey="index"
              size="small"
              pagination={false}
              style={{ marginTop: 16 }}
            />
          )}
        </div>
      )}
    </div>
  );
}

const STAGE_LABELS: Record<string, string> = {
  collect: '收集 URL',
  download: '下载图片',
  extract: '特征提取',
  milvus: '写入 Milvus',
  pg: '写入 PostgreSQL',
  done: '完成',
  error: '错误',
};

function FileImport() {
  const [file, setFile] = useState<File | null>(null);
  const [startLine, setStartLine] = useState(1);
  const [endLine, setEndLine] = useState(10000);
  const [concurrency, setConcurrency] = useState(8);
  const [retries, setRetries] = useState(2);
  const [skipKafka, setSkipKafka] = useState(true);
  const [running, setRunning] = useState(false);
  const [progress, setProgress] = useState<FileImportProgress | null>(null);
  const [logs, setLogs] = useState<string[]>([]);
  const [checkpoint, setCheckpoint] = useState<{ exists: boolean; stage?: string; done?: number; count?: number } | null>(null);
  const logRef = useRef<HTMLPreElement>(null);

  // Check for existing checkpoint on mount
  useEffect(() => {
    bffApi.getImportCheckpoint().then(({ data }) => setCheckpoint(data)).catch(() => {});
  }, []);

  const appendLog = useCallback((line: string) => {
    setLogs((prev) => {
      const next = [...prev, line];
      return next.length > 200 ? next.slice(-200) : next;
    });
  }, []);

  const consumeSSE = async (res: Response) => {
    if (!res.ok || !res.body) {
      let detail = `HTTP ${res.status}`;
      try { detail = (await res.text()) || detail; } catch {}
      message.error(`请求失败: ${detail}`);
      setRunning(false);
      return;
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';

    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });
      const lines = buffer.split('\n');
      buffer = lines.pop() || '';
      for (const line of lines) {
        if (line.startsWith('data: ')) {
          try {
            const data = JSON.parse(line.slice(6));
            if (data.type === 'log') {
              appendLog(data.message);
            } else if (data.stage) {
              setProgress(data as FileImportProgress);
              if (data.message) appendLog(`[${STAGE_LABELS[data.stage] || data.stage}] ${data.message}`);
            }
          } catch {}
        }
      }
    }
  };

  const handleStart = async () => {
    if (!file) {
      message.warning('请先上传 URL 列表文件');
      return;
    }
    setRunning(true);
    setProgress(null);
    setLogs([]);

    const formData = new FormData();
    formData.append('file', file);
    formData.append('params', JSON.stringify({ start: startLine, end: endLine, concurrency, retries, skip_kafka: skipKafka }));

    try {
      const res = await fetch(bffApi.fileImportUrl, {
        method: 'POST',
        body: formData,
      });
      await consumeSSE(res);
    } catch (e: any) {
      message.error('连接失败: ' + e.message);
    } finally {
      setRunning(false);
      setCheckpoint(null);
      if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  };

  const handleResume = async () => {
    setRunning(true);
    setProgress(null);
    setLogs([`正在恢复导入, 从阶段 "${STAGE_LABELS[checkpoint?.stage || ''] || checkpoint?.stage}" 继续, 已完成 ${checkpoint?.done || 0} 条...`]);

    try {
      const res = await fetch(bffApi.fileImportResumeUrl, { method: 'POST' });
      await consumeSSE(res);
    } catch (e: any) {
      message.error('恢复失败: ' + e.message);
    } finally {
      setRunning(false);
      setCheckpoint(null);
      if (logRef.current) logRef.current.scrollTop = logRef.current.scrollHeight;
    }
  };

  const percent = progress && progress.total > 0
    ? Math.round((progress.completed / progress.total) * 100)
    : 0;

  return (
    <div style={{ maxWidth: 700 }}>
      {checkpoint?.exists && !running && (
        <Alert
          type="warning"
          showIcon
          style={{ marginBottom: 16 }}
          message="检测到未完成的导入任务"
          description={`上次导入在「${STAGE_LABELS[checkpoint.stage || ''] || checkpoint.stage}」阶段中断，已完成 ${checkpoint.done?.toLocaleString()} / ${checkpoint.count?.toLocaleString()} 条。`}
          action={
            <Button type="primary" icon={<PlayCircleOutlined />} onClick={handleResume}>
              继续导入
            </Button>
          }
        />
      )}

      <Upload.Dragger
        accept=".txt,.csv"
        maxCount={1}
        beforeUpload={(f) => { setFile(f); return false; }}
        onRemove={() => setFile(null)}
        style={{ marginBottom: 24 }}
      >
        <p className="ant-upload-drag-icon"><FileTextOutlined /></p>
        <p className="ant-upload-text">点击或拖拽 URL 列表文件到此区域</p>
        <p className="ant-upload-hint">支持 .txt / .csv 文件，每行一个图片 URL</p>
      </Upload.Dragger>

      <Form layout="inline" style={{ marginBottom: 16, gap: '8px 0', flexWrap: 'wrap' }}>
        <Form.Item label="起始行">
          <InputNumber min={1} max={10000000} value={startLine} onChange={(v) => setStartLine(v || 1)} style={{ width: 120 }} />
        </Form.Item>
        <Form.Item label="结束行">
          <InputNumber min={1} max={10000000} value={endLine} onChange={(v) => setEndLine(v || 10000)} style={{ width: 120 }} />
        </Form.Item>
        <Form.Item label="并行数">
          <InputNumber min={1} max={64} value={concurrency} onChange={(v) => setConcurrency(v || 8)} style={{ width: 80 }} />
        </Form.Item>
        <Form.Item label="重试次数">
          <InputNumber min={0} max={10} value={retries} onChange={(v) => setRetries(v ?? 2)} style={{ width: 80 }} />
        </Form.Item>
        <Form.Item label="跳过 Kafka">
          <Switch checked={skipKafka} onChange={setSkipKafka} />
        </Form.Item>
      </Form>

      <Button type="primary" loading={running} onClick={handleStart} disabled={running || !file}>
        {running ? '导入中...' : '开始导入'}
      </Button>

      {progress && (
        <div style={{ marginTop: 16 }}>
          <div style={{ marginBottom: 8 }}>
            <Tag color={progress.stage === 'error' ? 'red' : progress.stage === 'done' ? 'green' : 'blue'}>
              {STAGE_LABELS[progress.stage] || progress.stage}
            </Tag>
            {progress.message}
          </div>
          <Progress
            percent={percent}
            status={progress.stage === 'error' ? 'exception' : running ? 'active' : 'success'}
            format={() => progress.total > 0 ? `${progress.completed}/${progress.total}` : `${percent}%`}
          />
        </div>
      )}

      {logs.length > 0 && (
        <pre
          ref={logRef}
          style={{
            marginTop: 16,
            maxHeight: 300,
            overflow: 'auto',
            background: '#1a1a1a',
            color: '#d4d4d4',
            padding: 12,
            borderRadius: 6,
            fontSize: 12,
            lineHeight: 1.5,
          }}
        >
          {logs.slice(-50).join('\n')}
        </pre>
      )}
    </div>
  );
}

export default function ImageImport() {
  return (
    <Card title="图片导入">
      <Tabs
        items={[
          { key: 'single', label: '单图导入', children: <SingleImport /> },
          { key: 'batch', label: '批量导入 (≤128)', children: <BatchImport /> },
          { key: 'file', label: '文件导入 (大批量)', children: <FileImport /> },
        ]}
      />
    </Card>
  );
}
