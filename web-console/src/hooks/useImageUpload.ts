import { useState } from 'react';
import { message } from 'antd';
import { bffApi } from '../api/bffApi';
import { isImageFile } from '../utils/imageUtils';

export function useImageUpload() {
  const [uploading, setUploading] = useState(false);
  const [uploadedUrl, setUploadedUrl] = useState<string | null>(null);

  const upload = async (file: File): Promise<string | null> => {
    if (!isImageFile(file)) {
      message.error('请上传图片文件');
      return null;
    }
    if (file.size > 10 * 1024 * 1024) {
      message.error('图片不能超过 10MB');
      return null;
    }
    setUploading(true);
    try {
      const { data } = await bffApi.uploadFile(file);
      setUploadedUrl(data.url);
      return data.url;
    } catch (e: any) {
      message.error('上传失败: ' + (e.response?.data?.message || e.message));
      return null;
    } finally {
      setUploading(false);
    }
  };

  return { upload, uploading, uploadedUrl, setUploadedUrl };
}
