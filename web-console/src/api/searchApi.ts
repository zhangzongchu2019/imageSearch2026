import client from './client';
import type { SearchRequest, SearchResponse } from './types';

export const searchApi = {
  search(data: SearchRequest) {
    return client.post<SearchResponse>('/api/v1/image/search', data);
  },
};
