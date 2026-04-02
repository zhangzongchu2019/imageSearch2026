#!/usr/bin/env bash
# download_models.sh — 下载 ViT-L-14 + FashionSigLIP 模型到 /data/imgsrch/models/
# 用法: bash scripts/download_models.sh
set -euo pipefail

MODEL_DIR="/data/imgsrch/models"
mkdir -p "$MODEL_DIR"

echo "$(date '+%Y-%m-%d %H:%M:%S') === 下载模型到 $MODEL_DIR ==="

# 1. ViT-L-14 (OpenAI CLIP, ~890MB)
VITL14_PATH="$MODEL_DIR/ViT-L-14.pt"
if [ -f "$VITL14_PATH" ]; then
    echo "ViT-L-14 already exists: $VITL14_PATH ($(du -h "$VITL14_PATH" | cut -f1))"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading ViT-L-14..."
    python3 -c "
import open_clip
model, _, _ = open_clip.create_model_and_transforms('ViT-L-14', pretrained='openai')
import torch
torch.save(model.state_dict(), '$VITL14_PATH')
print(f'Saved ViT-L-14 to $VITL14_PATH')
"
    echo "$(date '+%Y-%m-%d %H:%M:%S') ViT-L-14 downloaded: $(du -h "$VITL14_PATH" | cut -f1)"
fi

# 2. FashionSigLIP (Marqo, ~1.2GB)
FASHION_DIR="$MODEL_DIR/marqo-fashionsiglip"
if [ -d "$FASHION_DIR" ] && [ -f "$FASHION_DIR/open_clip_pytorch_model.bin" -o -f "$FASHION_DIR/model.safetensors" ]; then
    echo "FashionSigLIP already exists: $FASHION_DIR"
else
    echo "$(date '+%Y-%m-%d %H:%M:%S') Downloading FashionSigLIP..."
    python3 -c "
from huggingface_hub import snapshot_download
snapshot_download(
    repo_id='Marqo/marqo-fashionSigLIP',
    local_dir='$FASHION_DIR',
    local_dir_use_symlinks=False,
)
print(f'Saved FashionSigLIP to $FASHION_DIR')
"
    echo "$(date '+%Y-%m-%d %H:%M:%S') FashionSigLIP downloaded: $(du -sh "$FASHION_DIR" | cut -f1)"
fi

echo ""
echo "$(date '+%Y-%m-%d %H:%M:%S') === 模型下载完成 ==="
echo "ViT-L-14:        $VITL14_PATH"
echo "FashionSigLIP:   $FASHION_DIR"
du -sh "$MODEL_DIR"/*
