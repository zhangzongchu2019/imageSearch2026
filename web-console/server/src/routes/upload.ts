import { Router } from 'express';
import multer from 'multer';
import { v4 as uuid } from 'uuid';
import path from 'path';
import fs from 'fs';
import { config } from '../config.js';

const router = Router();

// Ensure upload dir exists
fs.mkdirSync(config.uploadDir, { recursive: true });

const storage = multer.diskStorage({
  destination: config.uploadDir,
  filename: (_req, file, cb) => {
    const ext = path.extname(file.originalname) || '.jpg';
    cb(null, `${uuid()}${ext}`);
  },
});

const upload = multer({
  storage,
  limits: { fileSize: 10 * 1024 * 1024 },
  fileFilter: (_req, file, cb) => {
    if (file.mimetype.startsWith('image/')) {
      cb(null, true);
    } else {
      cb(new Error('Only image files allowed'));
    }
  },
});

// Single file upload
router.post('/upload', upload.single('file'), (req, res) => {
  if (!req.file) {
    res.status(400).json({ message: 'No file uploaded' });
    return;
  }
  const url = `http://localhost:${config.port}/uploads/${req.file.filename}`;
  res.json({ url, filename: req.file.filename });
});

// Multi-file upload
router.post('/upload/multi', upload.array('files', 128), (req, res) => {
  const files = req.files as Express.Multer.File[];
  if (!files?.length) {
    res.status(400).json({ message: 'No files uploaded' });
    return;
  }
  const results = files.map((f) => ({
    url: `http://localhost:${config.port}/uploads/${f.filename}`,
    filename: f.filename,
    originalName: f.originalname,
  }));
  res.json({ files: results });
});

// Cleanup expired files every 10 minutes
setInterval(() => {
  const now = Date.now();
  try {
    const files = fs.readdirSync(config.uploadDir);
    for (const file of files) {
      const filePath = path.join(config.uploadDir, file);
      const stat = fs.statSync(filePath);
      if (now - stat.mtimeMs > config.uploadMaxAge) {
        fs.unlinkSync(filePath);
      }
    }
  } catch {}
}, 10 * 60_000);

export default router;
