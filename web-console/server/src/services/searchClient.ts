import axios from 'axios';
import { config } from '../config.js';

export const searchClient = axios.create({
  baseURL: config.searchServiceUrl,
  timeout: 10000,
});
