# 输出分类工作流升级说明

## 完成的任务

### 1. ✅ 清除原有向量数据
已删除以下文件：
- `data/vector_db/faiss.index`
- `data/vector_db/metadata.json`
- `data/vector_db/backend.json`

保留了 `.gitkeep` 文件。

### 2. ✅ 创建新的工作流脚本

创建了三个脚本：

#### a. `split_multi_topic_documents.py`（已有）
拆分单个多主题文档为多个独立主题文件。

#### b. `batch_split_documents.py`（已修改）
批量拆分目录中的多主题文档。

**新增功能：**
- 添加 `--output-dir` 参数，支持指定统一的输出目录
- 修复了类型注解兼容性问题（支持 Python 3.8+）

**用法：**
```bash
python batch_split_documents.py <input_dir> --output-dir <output_dir> [--move-originals] [--dry-run]
```

#### c. `output_classification_workflow.py`（新建）
输出分类的完整工作流wrapper脚本。

**执行三个步骤：**
1. **拆分多主题文档**：`summarize_dir` → `split_dir`
   - 检测包含"# 主题 N："格式的多主题文档
   - 拆分为独立的单主题文件
   - 成功拆分后将原文件移至 `archived_multi_topic/`

2. **分类拆分后的文档**：`split_dir` → `documents/`
   - 根据元数据中的 `categories` 字段分类
   - 移动到对应的 `documents/<category>/` 目录

3. **分类剩余文档**：`summarize_dir` → `documents/`
   - 处理单主题文档和拆分失败的文档
   - 同样根据 `categories` 分类移动

**用法：**
```bash
python output_classification_workflow.py [--dry-run]
```

### 3. ✅ 修改GUI集成

修改了 `gui_launcher.py` 中的 `run_output_classification()` 函数：

**原逻辑：**
- 直接调用 `move_summaries_by_category.py`
- 从 `summarize_dir` 移动到 `documents/`

**新逻辑：**
- 调用 `output_classification_workflow.py`
- 执行完整的三步工作流
- 在日志中显示详细步骤说明

**GUI帮助文档也已更新**，清晰说明三步流程。

### 4. ✅ 修复兼容性问题

修复了所有脚本的类型注解问题，确保兼容 Python 3.8+：
- `batch_split_documents.py`：添加 `from typing import Optional`
- `output_classification_workflow.py`：添加 `from typing import List`
- `move_summaries_by_category.py`：添加 `from typing import List, Optional`
- 将 `list[str]` 改为 `List[str]`
- 将 `str | None` 改为 `Optional[str]`

## 测试结果

### 预览模式测试
```bash
python scripts/output_classification_workflow.py --dry-run
```

**测试数据：**
- 源文件：313个markdown文件
- 检测到1个多主题文档（`250401_食物能量与热量的科学辨析.md`）
- 拆分为2个独立主题文件

**测试结果：**
- ✅ 步骤1 (拆分): 成功
- ✅ 步骤2 (分类split_dir): 成功 - 处理2个文件
- ✅ 步骤3 (分类summarize_dir): 成功 - 处理314个文件（312单主题 + 1拆分失败 + 1原多主题文档已移除）

## 使用指南

### 完整工作流程

1. **AI总结**：生成文档到 `data/summarize_dir/`
   
2. **输出分类**（点击GUI按钮）：
   - 自动拆分多主题文档
   - 自动分类所有文档到 `documents/`
   
3. **补齐向量**：为 `documents/` 中的新文档建立索引

### 手动执行（可选）

如果需要手动执行或调试：

```bash
# 1. 预览拆分效果
python scripts/batch_split_documents.py data/summarize_dir --output-dir data/split_dir --dry-run

# 2. 执行拆分
python scripts/batch_split_documents.py data/summarize_dir --output-dir data/split_dir --move-originals

# 3. 分类文档
python scripts/move_summaries_by_category.py --input-dir data/split_dir --documents-dir documents
python scripts/move_summaries_by_category.py --input-dir data/summarize_dir --documents-dir documents

# 或者使用工作流脚本一次执行
python scripts/output_classification_workflow.py
```

## 目录结构

```
ai_conversations_summary/
├── data/
│   ├── summarize_dir/          # AI总结输出目录
│   │   └── archived_multi_topic/  # 已拆分的多主题文档归档
│   ├── split_dir/              # 拆分后的文档临时目录
│   └── vector_db/              # 向量数据库（已清空）
├── documents/                  # 分类后的文档目录
│   ├── science/
│   ├── humanities/
│   ├── industry-tech/
│   ├── finance/
│   └── ...
└── scripts/
    ├── split_multi_topic_documents.py      # 单文件拆分
    ├── batch_split_documents.py            # 批量拆分
    ├── output_classification_workflow.py   # 工作流wrapper
    ├── move_summaries_by_category.py       # 分类移动
    └── gui_launcher.py                     # GUI主程序
```

## 注意事项

1. **向量数据库清空**：已删除所有旧的embedding数据，需要重新执行"补齐向量"

2. **文档归档**：拆分成功的多主题文档会被移动到 `summarize_dir/archived_multi_topic/`，不会被删除

3. **拆分失败处理**：无法拆分的文档（单主题或格式不符）会保留在 `summarize_dir`，随后被正常分类

4. **重复运行安全**：工作流可以安全地重复运行，已处理的文件会被自动跳过

5. **预览模式**：建议首次使用时添加 `--dry-run` 参数预览效果

## 文件变更清单

### 新建文件
- `scripts/output_classification_workflow.py`

### 修改文件
- `scripts/batch_split_documents.py`：添加 `--output-dir` 参数
- `scripts/move_summaries_by_category.py`：修复类型注解
- `scripts/gui_launcher.py`：修改 `run_output_classification()` 函数及帮助文档

### 删除文件
- `data/vector_db/faiss.index`
- `data/vector_db/metadata.json`
- `data/vector_db/backend.json`

## 下一步操作

1. 在GUI中点击"输出分类"按钮，执行新的工作流
2. 检查 `documents/` 目录中的文档是否正确分类
3. 点击"补齐向量"，重建向量索引
4. 在"预览"或"RAG Q&A"中测试检索功能
