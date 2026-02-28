import axios from 'axios';
import { config } from '../config.js';

export const writeClient = axios.create({
  baseURL: config.writeServiceUrl,
  timeout: 30000,
});
