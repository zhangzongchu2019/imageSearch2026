import { useState, useEffect, useCallback } from 'react';
import { Card, Tabs, Table, Tag, message, Spin, Typography } from 'antd';
import type { ColumnsType } from 'antd/es/table';
import { bffApi } from '../../../api/bffApi';
import type { MilvusPartitionInfo, MilvusRecord } from '../../../api/types';

const PAGE_SIZE = 200;
const MAX_TOTAL = 1000;

const columns: ColumnsType<MilvusRecord> = [
  { title: 'image_pk', dataIndex: 'image_pk', width: 280, ellipsis: true },
  { title: 'product_id', dataIndex: 'product_id', width: 140 },
  {
    title: 'category_l1',
    dataIndex: 'category_l1',
    width: 100,
    align: 'center',
  },
  {
    title: 'category_l2',
    dataIndex: 'category_l2',
    width: 100,
    align: 'center',
  },
  {
    title: 'tags',
    dataIndex: 'tags',
    width: 200,
    render: (tags: number[]) =>
      tags?.length ? tags.slice(0, 5).map((t) => <Tag key={t}>{t}</Tag>) : '-',
  },
  {
    title: 'is_evergreen',
    dataIndex: 'is_evergreen',
    width: 110,
    align: 'center',
    render: (v: boolean) => (v ? <Tag color="green">是</Tag> : <Tag>否</Tag>),
  },
  { title: 'ts_month', dataIndex: 'ts_month', width: 100, align: 'center' },
  { title: 'vec_dim', dataIndex: 'vec_dim', width: 80, align: 'center' },
];

export default function DataBrowser() {
  const [partitions, setPartitions] = useState<MilvusPartitionInfo[]>([]);
  const [activePartition, setActivePartition] = useState<string>('');
  const [records, setRecords] = useState<MilvusRecord[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loadingPartitions, setLoadingPartitions] = useState(false);
  const [loadingData, setLoadingData] = useState(false);

  const fetchPartitions = useCallback(async () => {
    setLoadingPartitions(true);
    try {
      const { data } = await bffApi.getMilvusPartitions();
      setPartitions(data);
      if (data.length > 0 && !activePartition) {
        setActivePartition(data[0].name);
      }
    } catch (e: any) {
      message.error('获取分区列表失败: ' + e.message);
    } finally {
      setLoadingPartitions(false);
    }
  }, [activePartition]);

  const fetchData = useCallback(async (partition: string, pageNum: number) => {
    setLoadingData(true);
    try {
      const offset = (pageNum - 1) * PAGE_SIZE;
      const { data } = await bffApi.getMilvusData(partition, offset, PAGE_SIZE);
      setRecords(data.records);
      setTotal(Math.min(data.total, MAX_TOTAL));
    } catch (e: any) {
      message.error('获取数据失败: ' + e.message);
    } finally {
      setLoadingData(false);
    }
  }, []);

  useEffect(() => {
    fetchPartitions();
  }, [fetchPartitions]);

  useEffect(() => {
    if (activePartition) {
      setPage(1);
      fetchData(activePartition, 1);
    }
  }, [activePartition, fetchData]);

  const currentPartition = partitions.find((p) => p.name === activePartition);

  return (
    <Card title="Milvus 数据浏览">
      <Spin spinning={loadingPartitions && partitions.length === 0}>
        <Tabs
          activeKey={activePartition}
          onChange={(key) => setActivePartition(key)}
          items={partitions.map((p) => ({
            key: p.name,
            label: `${p.name} (${p.count.toLocaleString()})`,
          }))}
        />

        {currentPartition && (
          <div style={{ marginBottom: 16 }}>
            <Typography.Text type="secondary">
              本分区数据总量: <strong>{currentPartition.count.toLocaleString()}</strong> 条
              {currentPartition.lastUpdated && (
                <>
                  {' | '}最后更新时间:{' '}
                  <strong>{new Date(currentPartition.lastUpdated).toLocaleString()}</strong>
                </>
              )}
            </Typography.Text>
          </div>
        )}

        <Table<MilvusRecord>
          rowKey="image_pk"
          columns={columns}
          dataSource={records}
          loading={loadingData}
          size="small"
          scroll={{ x: 1200 }}
          pagination={{
            current: page,
            pageSize: PAGE_SIZE,
            total,
            showTotal: (t) => `共 ${t} 条（最多展示 ${MAX_TOTAL} 条）`,
            onChange: (p) => {
              setPage(p);
              fetchData(activePartition, p);
            },
          }}
        />
      </Spin>
    </Card>
  );
}
