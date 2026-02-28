import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface AuthState {
  apiKey: string;
  setApiKey: (key: string) => void;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      apiKey: '',
      setApiKey: (apiKey) => set({ apiKey }),
    }),
    { name: 'imgsrch-auth' },
  ),
);
