import React, { useState } from 'react';
import { useNavigate, useLocation } from 'react-router-dom';
import { Layout, Menu, Typography, Input, theme } from 'antd';
import {
  DashboardOutlined,
  UploadOutlined,
  SearchOutlined,
  HistoryOutlined,
  SettingOutlined,
  ExperimentOutlined,
  ClockCircleOutlined,
  ThunderboltOutlined,
  ApiOutlined,
  KeyOutlined,
} from '@ant-design/icons';
import type { MenuProps } from 'antd';
import { useAuthStore } from '../../stores/authStore';

const { Header, Sider, Content } = Layout;

const menuItems: MenuProps['items'] = [
  { key: '/', icon: <DashboardOutlined />, label: 'Dashboard' },
  { key: '/import', icon: <UploadOutlined />, label: '图片导入' },
  { key: '/search', icon: <SearchOutlined />, label: '图片查询' },
  { key: '/history', icon: <HistoryOutlined />, label: '历史记录' },
  {
    key: 'admin',
    icon: <SettingOutlined />,
    label: '运维管理',
    children: [
      { key: '/admin/degrade', icon: <ThunderboltOutlined />, label: '降级管理' },
      { key: '/admin/breakers', icon: <ApiOutlined />, label: '熔断器' },
      { key: '/admin/config', icon: <SettingOutlined />, label: '配置管理' },
      { key: '/admin/tests', icon: <ExperimentOutlined />, label: '测试运行' },
      { key: '/admin/scheduler', icon: <ClockCircleOutlined />, label: '任务调度' },
    ],
  },
];

export default function AppLayout({ children }: { children: React.ReactNode }) {
  const [collapsed, setCollapsed] = useState(false);
  const navigate = useNavigate();
  const location = useLocation();
  const { token: themeToken } = theme.useToken();
  const { apiKey, setApiKey } = useAuthStore();

  const onClick: MenuProps['onClick'] = ({ key }) => {
    navigate(key);
  };

  const selectedKeys = [location.pathname];
  const openKeys = location.pathname.startsWith('/admin') ? ['admin'] : [];

  return (
    <Layout style={{ minHeight: '100vh' }}>
      <Sider
        collapsible
        collapsed={collapsed}
        onCollapse={setCollapsed}
        theme="light"
        style={{ borderRight: `1px solid ${themeToken.colorBorderSecondary}` }}
      >
        <div style={{ padding: '16px', textAlign: 'center' }}>
          <Typography.Title level={collapsed ? 5 : 4} style={{ margin: 0 }}>
            {collapsed ? 'IS' : 'ImageSearch'}
          </Typography.Title>
        </div>
        <Menu
          mode="inline"
          selectedKeys={selectedKeys}
          defaultOpenKeys={openKeys}
          items={menuItems}
          onClick={onClick}
        />
      </Sider>
      <Layout>
        <Header
          style={{
            background: themeToken.colorBgContainer,
            padding: '0 24px',
            display: 'flex',
            alignItems: 'center',
            justifyContent: 'flex-end',
            borderBottom: `1px solid ${themeToken.colorBorderSecondary}`,
          }}
        >
          <Input
            prefix={<KeyOutlined />}
            placeholder="API Key"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            style={{ width: 300 }}
            type="password"
          />
        </Header>
        <Content style={{ margin: 16, padding: 24, background: themeToken.colorBgContainer, borderRadius: 8 }}>
          {children}
        </Content>
      </Layout>
    </Layout>
  );
}
