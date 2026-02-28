import axios from 'axios';
import { useAuthStore } from '../stores/authStore';

const client = axios.create({
  timeout: 30000,
});

client.interceptors.request.use((config) => {
  const apiKey = useAuthStore.getState().apiKey;
  if (apiKey) {
    config.headers['X-API-Key'] = apiKey;
  }
  return config;
});

client.interceptors.response.use(
  (response) => response,
  (error) => {
    if (error.response?.status === 401) {
      console.error('API Key invalid or missing');
    }
    return Promise.reject(error);
  },
);

export default client;
