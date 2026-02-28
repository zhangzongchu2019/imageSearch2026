import { useState } from 'react';
import { Tabs, Card, Upload, Form, Input, Button, Select, message, Table, Tag, Progress, Alert } from 'antd';
import { InboxOutlined, UploadOutlined } from '@ant-design/icons';
import type { UploadFile } from 'antd';
import { useImageUpload } from '../../hooks/useImageUpload';
import { useBatchOperation } from '../../hooks/useBatchOperation';
import { bffApi } from '../../api/bffApi';
import { useHistoryStore } from '../../stores/historyStore';

const { Dragger } = Upload;
const { TextArea } = Input;

const categories = [
  'clothing', 'shoes', 'bags', 'accessories', 'electronics',
  'home', 'beauty', 'food', 'sports', 'toys',
];

function SingleImport() {
  const [form] = Form.useForm();
  const { upload, uploading, uploadedUrl } = useImageUpload();
  const [submitting, setSubmitting] = useState(false);
  const [result, setResult] = useState<any>(null);
  const addRecord = useHistoryStore((s) => s.addRecord);

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
        <Form.Item name="merchant_id" label="商家 ID" rules={[{ required: true }]}>
          <Input placeholder="merchant_001" />
        </Form.Item>
        <Form.Item name="category_l1" label="一级分类" rules={[{ required: true }]}>
          <Select options={categories.map((c) => ({ label: c, value: c }))} placeholder="选择分类" />
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
  const { progress, running, start } = useBatchOperation('/api/bff/batch/import');

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

export default function ImageImport() {
  return (
    <Card title="图片导入">
      <Tabs
        items={[
          { key: 'single', label: '单图导入', children: <SingleImport /> },
          { key: 'batch', label: '批量导入 (≤128)', children: <BatchImport /> },
        ]}
      />
    </Card>
  );
}
