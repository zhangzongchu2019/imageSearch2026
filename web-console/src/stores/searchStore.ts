import { create } from 'zustand';
import type { SearchResponse, SearchResultItem } from '../api/types';

interface SearchState {
  lastResponse: SearchResponse | null;
  selectedResult: SearchResultItem | null;
  queryImageUrl: string | null;
  setLastResponse: (resp: SearchResponse) => void;
  setSelectedResult: (item: SearchResultItem | null) => void;
  setQueryImageUrl: (url: string | null) => void;
}

export const useSearchStore = create<SearchState>((set) => ({
  lastResponse: null,
  selectedResult: null,
  queryImageUrl: null,
  setLastResponse: (lastResponse) => set({ lastResponse }),
  setSelectedResult: (selectedResult) => set({ selectedResult }),
  setQueryImageUrl: (queryImageUrl) => set({ queryImageUrl }),
}));
