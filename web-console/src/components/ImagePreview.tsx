import { Image } from 'antd';

interface Props {
  src: string;
  width?: number;
  height?: number;
}

export default function ImagePreview({ src, width = 120, height = 120 }: Props) {
  return (
    <Image
      src={src}
      width={width}
      height={height}
      style={{ objectFit: 'cover', borderRadius: 4 }}
      fallback="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTIwIiBoZWlnaHQ9IjEyMCIgeG1sbnM9Imh0dHA6Ly93d3cudzMub3JnLzIwMDAvc3ZnIj48cmVjdCB3aWR0aD0iMTIwIiBoZWlnaHQ9IjEyMCIgZmlsbD0iI2YwZjBmMCIvPjx0ZXh0IHg9IjUwJSIgeT0iNTAlIiBkb21pbmFudC1iYXNlbGluZT0ibWlkZGxlIiB0ZXh0LWFuY2hvcj0ibWlkZGxlIiBmaWxsPSIjYmZiZmJmIj5ObyBJbWFnZTwvdGV4dD48L3N2Zz4="
    />
  );
}
