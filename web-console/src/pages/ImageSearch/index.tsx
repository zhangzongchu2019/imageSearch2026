import { useState } from 'react';
import {
  Card, Tabs, Upload, Form, InputNumber, Select, Button, Row, Col, Tag, Descriptions,
  Modal, Slider, Table, Progress, message, Image, Space, Typography,
} from 'antd';
import { InboxOutlined, SearchOutlined, ZoomInOutlined } from '@ant-design/icons';
import { searchApi } from '../../api/searchApi';
import { useSearchStore } from '../../stores/searchStore';
import { useHistoryStore } from '../../stores/historyStore';
import { useBatchOperation } from '../../hooks/useBatchOperation';
import { fileToBase64, fileToDataUrl } from '../../utils/imageUtils';
import { formatMs } from '../../utils/formatters';
import ConfidenceBadge from '../../components/ConfidenceBadge';
import type { SearchResultItem, SearchResponse } from '../../api/types';

const { Text } = Typography;

function SingleSearch() {
  const [form] = Form.useForm();
  const [searching, setSearching] = useState(false);
  const [queryPreview, setQueryPreview] = useState<string | null>(null);
  const { lastResponse, setLastResponse, selectedResult, setSelectedResult, setQueryImageUrl } = useSearchStore();
  const addRecord = useHistoryStore((s) => s.addRecord);
  const [compareVisible, setCompareVisible] = useState(false);

  const handleSearch = async (file: File) => {
    setSearching(true);
    try {
      const [base64, dataUrl] = await Promise.all([fileToBase64(file), fileToDataUrl(file)]);
      setQueryPreview(dataUrl);
      setQueryImageUrl(dataUrl);
      const params = form.getFieldsValue();
      const { data } = await searchApi.search({
        query_image: base64,
        top_k: params.top_k,
        merchant_scope: params.merchant_scope?.length ? params.merchant_scope : undefined,
        data_scope: params.data_scope,
        time_range: params.time_range,
      });
      setLastResponse(data);
      addRecord({ type: 'search', params: { top_k: params.top_k }, result: { total: data.meta.total_results, strategy: data.meta.strategy } });
    } catch (e: any) {
      message.error('查询失败: ' + (e.response?.data?.error?.message || e.message));
    } finally {
      setSearching(false);
    }
  };

  const openCompare = (item: SearchResultItem) => {
    setSelectedResult(item);
    setCompareVisible(true);
  };

  return (
    <div>
      <Row gutter={24}>
        <Col span={8}>
          <Upload.Dragger
            accept="image/*"
            maxCount={1}
            beforeUpload={(file) => { handleSearch(file); return false; }}
            showUploadList={false}
            style={{ marginBottom: 16 }}
          >
            {queryPreview ? (
              <img src={queryPreview} alt="query" style={{ maxWidth: '100%', maxHeight: 200 }} />
            ) : (
              <>
                <p className="ant-upload-drag-icon"><InboxOutlined /></p>
                <p className="ant-upload-text">拖拽或点击上传查询图片</p>
              </>
            )}
          </Upload.Dragger>

          <Form form={form} layout="vertical" initialValues={{ top_k: 20 }}>
            <Form.Item name="top_k" label="Top K">
              <InputNumber min={1} max={200} style={{ width: '100%' }} />
            </Form.Item>
            <Form.Item name="merchant_scope" label="商家范围">
              <Select mode="tags" placeholder="输入商家 ID" />
            </Form.Item>
            <Form.Item name="data_scope" label="数据范围">
              <Select allowClear options={[
                { label: '全部', value: 'ALL' },
                { label: '滚动区', value: 'ROLLING' },
                { label: '常青区', value: 'EVERGREEN' },
              ]} />
            </Form.Item>
            <Form.Item name="time_range" label="时间范围">
              <Select allowClear options={[
                { label: '仅热区 (5月)', value: 'HOT_ONLY' },
                { label: '全部 (18月+)', value: 'ALL' },
              ]} />
            </Form.Item>
          </Form>
        </Col>

        <Col span={16}>
          {searching && <Card loading style={{ height: 200 }} />}

          {lastResponse && !searching && (
            <>
              {/* Performance panel */}
              <Card size="small" style={{ marginBottom: 16 }}>
                <Descriptions size="small" column={4}>
                  <Descriptions.Item label="策略">{lastResponse.meta.strategy}</Descriptions.Item>
                  <Descriptions.Item label="置信度"><ConfidenceBadge confidence={lastResponse.meta.confidence} /></Descriptions.Item>
                  <Descriptions.Item label="总耗时">{formatMs(lastResponse.meta.latency_ms)}</Descriptions.Item>
                  <Descriptions.Item label="结果数">{lastResponse.meta.total_results}</Descriptions.Item>
                  <Descriptions.Item label="特征提取">{formatMs(lastResponse.meta.feature_ms)}</Descriptions.Item>
                  <Descriptions.Item label="ANN(热)">{formatMs(lastResponse.meta.ann_hot_ms)}</Descriptions.Item>
                  <Descriptions.Item label="过滤">{formatMs(lastResponse.meta.filter_ms)}</Descriptions.Item>
                  <Descriptions.Item label="精排">{formatMs(lastResponse.meta.refine_ms)}</Descriptions.Item>
                </Descriptions>
                {lastResponse.meta.degraded && (
                  <Tag color="orange" style={{ marginTop: 4 }}>降级: {lastResponse.meta.degrade_state} - {lastResponse.meta.degrade_reason}</Tag>
                )}
              </Card>

              {/* Results grid */}
              <Row gutter={[12, 12]}>
                {lastResponse.results.map((item) => (
                  <Col key={item.image_id} xs={12} sm={8} md={6}>
                    <Card
                      size="small"
                      hoverable
                      onClick={() => openCompare(item)}
                      cover={
                        <div style={{ height: 120, background: '#f5f5f5', display: 'flex', alignItems: 'center', justifyContent: 'center' }}>
                          <Text type="secondary" style={{ fontSize: 10 }}>{item.image_id.slice(0, 12)}</Text>
                        </div>
                      }
                    >
                      <div>
                        <div style={{ display: 'flex', justifyContent: 'space-between', marginBottom: 4 }}>
                          <Text strong style={{ fontSize: 12 }}>#{item.position}</Text>
                          <Text style={{ fontSize: 12 }}>{(item.score * 100).toFixed(1)}%</Text>
                        </div>
                        <Tag style={{ fontSize: 10 }}>{item.category_l1}</Tag>
                        {item.is_evergreen && <Tag color="green" style={{ fontSize: 10 }}>常青</Tag>}
                      </div>
                    </Card>
                  </Col>
                ))}
              </Row>
            </>
          )}
        </Col>
      </Row>

      {/* Compare modal */}
      <Modal
        open={compareVisible}
        onCancel={() => setCompareVisible(false)}
        width={900}
        title="结果对比"
        footer={null}
      >
        {selectedResult && (
          <Row gutter={24}>
            <Col span={12}>
              <Card title="查询图片" size="small">
                {queryPreview && <img src={queryPreview} alt="query" style={{ width: '100%' }} />}
              </Card>
            </Col>
            <Col span={12}>
              <Card title="匹配结果" size="small">
                <Descriptions column={1} size="small">
                  <Descriptions.Item label="图片 ID">{selectedResult.image_id}</Descriptions.Item>
                  <Descriptions.Item label="相似度">
                    <Progress percent={Math.round(selectedResult.score * 100)} size="small" />
                  </Descriptions.Item>
                  <Descriptions.Item label="商品 ID">{selectedResult.product_id}</Descriptions.Item>
                  <Descriptions.Item label="分类">{selectedResult.category_l1}</Descriptions.Item>
                  <Descriptions.Item label="标签">
                    <Space wrap>{selectedResult.tags.map((t) => <Tag key={t}>{t}</Tag>)}</Space>
                  </Descriptions.Item>
                  <Descriptions.Item label="排名">#{selectedResult.position}</Descriptions.Item>
                  <Descriptions.Item label="常青">{selectedResult.is_evergreen ? '是' : '否'}</Descriptions.Item>
                </Descriptions>
              </Card>
            </Col>
          </Row>
        )}
      </Modal>
    </div>
  );
}

function BatchSearch() {
  const { progress, running, start } = useBatchOperation('/api/bff/batch/search');
  const [activeTab, setActiveTab] = useState('0');

  const handleStart = (fileList: File[]) => {
    const formData = new FormData();
    fileList.forEach((f) => formData.append('files', f));
    formData.append('params', JSON.stringify({ top_k: 20 }));
    start(formData);
  };

  const results = (progress as any)?.results || [];

  return (
    <div>
      <Upload.Dragger
        accept="image/*"
        multiple
        maxCount={128}
        beforeUpload={() => false}
        onChange={(info) => {
          if (info.fileList.length > 0) {
            handleStart(info.fileList.map((f) => f.originFileObj!));
          }
        }}
        style={{ marginBottom: 16 }}
      >
        <p className="ant-upload-drag-icon"><InboxOutlined /></p>
        <p className="ant-upload-text">拖拽多张图片开始批量查询 (最多128张)</p>
      </Upload.Dragger>

      {progress && (
        <div>
          <Progress
            percent={progress.total ? Math.round((progress.completed / progress.total) * 100) : 0}
            status={running ? 'active' : undefined}
          />
          {results.length > 0 && (
            <Tabs activeKey={activeTab} onChange={setActiveTab}>
              {results.map((r: any) => (
                <Tabs.TabPane key={String(r.index)} tab={`图片 #${r.index + 1}`}>
                  {r.success ? (
                    <Descriptions size="small" column={3}>
                      <Descriptions.Item label="结果数">{r.data?.meta?.total_results}</Descriptions.Item>
                      <Descriptions.Item label="策略">{r.data?.meta?.strategy}</Descriptions.Item>
                      <Descriptions.Item label="耗时">{formatMs(r.data?.meta?.latency_ms || 0)}</Descriptions.Item>
                    </Descriptions>
                  ) : (
                    <Tag color="red">{r.error}</Tag>
                  )}
                </Tabs.TabPane>
              ))}
            </Tabs>
          )}
        </div>
      )}
    </div>
  );
}

export default function ImageSearchPage() {
  return (
    <Card title="图片查询">
      <Tabs
        items={[
          { key: 'single', label: '单图查询', children: <SingleSearch /> },
          { key: 'batch', label: '批量查询 (≤128)', children: <BatchSearch /> },
        ]}
      />
    </Card>
  );
}
