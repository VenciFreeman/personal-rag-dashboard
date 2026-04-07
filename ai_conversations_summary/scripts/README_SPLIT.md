# 多主题文档拆分工具

本工具用于将单个多主题Markdown 文档拆分为多个单主题文档，便于后续索引重建与检索质量提升。

## 1. 适用场景

- 单篇文档包含多个主题块，导致向量检索召回粒度过粗。
- 需要把每个主题独立建索引，提升 rerank 与引用精度。

## 2. 实现思路

1. 解析原文中的主题段落（按约定标题模式）。
2. 为每个主题生成独立文件。
3. 同步更新基础元数据（标题、标签、摘要等）。
4. 输出到目标目录，供后续索引脚本使用。

## 3. 脚本说明

- `split_multi_topic_documents.py`：拆分单个文件。
- `data_maintenance/batch_split_documents.py`：批量扫描目录并拆分。

## 4. 使用方式

### 4.1 单文件拆分

```bash
python split_multi_topic_documents.py <input_file> [--output-dir <dir>] [--dry-run]
```

### 4.2 批量拆分

```bash
python data_maintenance/batch_split_documents.py <documents_dir> [--dry-run] [--move-originals] [--no-recursive]
```

如果在当前仓库统一虚拟环境下执行，推荐使用：

```powershell
..\..\.venv\Scripts\python.exe data_maintenance\batch_split_documents.py <documents_dir> --dry-run
```

## 5. 推荐流程

1. 先执行 `--dry-run` 预览结果。
2. 确认主题识别正确后执行实际拆分。
3. 复核输出目录内容。
4. 运行索引构建脚本更新向量库。

## 6. 注意事项

- 不满足主题标题约定的文档会被跳过。
- `--move-originals` 会移动原文档，建议先做预览。
- 文件名可能因平台限制进行长度裁剪。
- 文档拆分会直接影响后续 RAG 与 Dashboard Agent 的召回粒度，因此建议在大批量拆分后重建索引并做回归验证。
