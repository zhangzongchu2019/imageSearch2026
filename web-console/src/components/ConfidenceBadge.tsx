import { Tag } from 'antd';
import type { Confidence } from '../api/types';

const colorMap: Record<Confidence, string> = {
  HIGH: 'green',
  MEDIUM: 'orange',
  LOW: 'red',
};

export default function ConfidenceBadge({ confidence }: { confidence: Confidence }) {
  return <Tag color={colorMap[confidence]}>{confidence}</Tag>;
}
