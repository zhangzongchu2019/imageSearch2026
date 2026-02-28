import client from './client';
import type { UpdateImageRequest, UpdateImageResponse } from './types';

export const writeApi = {
  updateImage(data: UpdateImageRequest) {
    return client.post<UpdateImageResponse>('/api/v1/image/update', data);
  },
};
