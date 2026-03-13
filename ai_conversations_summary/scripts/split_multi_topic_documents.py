"""
拆分多主题会话文档的脚本

将包含多个主题的markdown文档拆分为独立文件。
每个新文件包含：元数据 + 主题内容 + 关键词标签

用法：
    python split_multi_topic_documents.py <input_file> [--output-dir <dir>] [--dry-run]
    
示例：
    python split_multi_topic_documents.py ../documents/science/260220_传统经验与现代科学的解构分析.md --dry-run
"""

import re
import argparse
from pathlib import Path
from typing import List, Tuple, Dict, Optional


DATE_IN_TEXT_RE = re.compile(r"(\d{4})[-_](\d{2})[-_](\d{2})")
FILENAME_YYMMDD_RE = re.compile(r"^(\d{6})(?:[_-]|$)")
FILENAME_YYYYMMDD_RE = re.compile(r"^(\d{8})(?:[_-]|$)")
FILENAME_YYYY_MM_DD_RE = re.compile(r"^(\d{4})[-_](\d{2})[-_](\d{2})(?:[-_]\d{4,6})?$")
TOPIC_HEADER_RE = re.compile(r"^#{1,3}\s*主题\s*([0-9一二三四五六七八九十]+)\s*[：:]\s*(.+?)\s*$", re.MULTILINE)


def parse_frontmatter(lines: List[str]) -> Tuple[Dict[str, str], int]:
    """解析文档前置元数据"""
    metadata = {}
    i = 0
    
    # 跳过空行
    while i < len(lines) and not lines[i].strip():
        i += 1
    
    # 解析元数据（以 > - **key**: value 格式）
    while i < len(lines) and lines[i].startswith('>'):
        line = lines[i].strip()
        # 匹配格式：> - **title**: 传统经验与现代科学的解构分析
        match = re.match(r'>\s*-\s*\*\*(\w+)\*\*:\s*(.+)', line)
        if match:
            key, value = match.groups()
            # 清理引号
            value = value.strip().strip('"\'')
            metadata[key] = value
        i += 1
    
    return metadata, i


def extract_topic_tags(content: str) -> Optional[str]:
    """从主题内容末尾提取标签"""
    # 查找最后一个 ## 标签 部分
    match = re.search(r'##\s*标签\s*\n(.+?)(?:\n------|$)', content, re.DOTALL)
    if match:
        tags_text = match.group(1).strip()
        return tags_text
    return None


def sanitize_filename(text: str) -> str:
    """清理文件名中的非法字符"""
    # 移除或替换文件名非法字符
    text = re.sub(r'[<>:"/\\|?*]', '', text)
    text = re.sub(r'[\s]+', '', text)
    # 限制长度
    if len(text) > 80:
        text = text[:80]
    return text


def _date_prefix_from_filename(file_path: Path) -> str:
    stem = file_path.stem.strip()
    if not stem:
        return ""
    m6 = FILENAME_YYMMDD_RE.match(stem)
    if m6:
        return m6.group(1)
    m8 = FILENAME_YYYYMMDD_RE.match(stem)
    if m8:
        value = m8.group(1)
        return f"{value[2:4]}{value[4:6]}{value[6:8]}"
    mdash = FILENAME_YYYY_MM_DD_RE.match(stem)
    if mdash:
        yyyy, mm, dd = mdash.groups()
        return f"{yyyy[2:]}{mm}{dd}"
    return ""


def _date_prefix_from_metadata(metadata: Dict[str, str]) -> str:
    raw = str(metadata.get("date", "") or "").strip().strip('"\'')
    if not raw:
        return ""

    m = DATE_IN_TEXT_RE.search(raw)
    if m:
        yyyy, mm, dd = m.groups()
        return f"{yyyy[2:]}{mm}{dd}"

    compact = re.search(r"\b(\d{8})\b", raw)
    if compact:
        value = compact.group(1)
        return f"{value[2:4]}{value[4:6]}{value[6:8]}"

    short = re.search(r"\b(\d{6})\b", raw)
    if short:
        return short.group(1)

    return ""


def split_document(file_path: Path, output_dir: Optional[Path] = None, dry_run: bool = False) -> List[Path]:
    """拆分多主题文档"""
    
    # 读取文件
    with open(file_path, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    # 解析元数据
    original_metadata, metadata_end = parse_frontmatter(lines)
    
    # 日期优先级：元数据 date -> 文件名日期 -> 兜底 000000
    date_prefix = _date_prefix_from_metadata(original_metadata) or _date_prefix_from_filename(file_path)
    if not date_prefix:
        print(f"警告：无法从元数据/文件名提取日期前缀: {file_path.name}")
        date_prefix = "000000"
    
    # 合并剩余内容
    content = ''.join(lines[metadata_end:])
    
    # 查找所有主题，兼容 #/##/### 级标题与中文数字。
    topics = list(TOPIC_HEADER_RE.finditer(content))
    
    if len(topics) == 0:
        print(f"未检测到多主题结构（应包含 '# 主题 N：标题' 格式）")
        return []
    
    if len(topics) == 1:
        # 单主题场景也做一次规范化输出，去掉“本文概览/主题1”残留结构。
        print("只检测到1个主题，将执行单主题规范化输出")
    else:
        print(f"检测到 {len(topics)} 个主题")
    
    # 确定输出目录
    if output_dir is None:
        output_dir = file_path.parent / "split_output"
    output_dir.mkdir(parents=True, exist_ok=True)
    
    created_files = []
    
    # 拆分每个主题
    for idx, topic_match in enumerate(topics):
        topic_num = topic_match.group(1)
        topic_title = topic_match.group(2).strip()
        
        # 提取主题内容
        start_pos = topic_match.start()
        if idx < len(topics) - 1:
            # 不是最后一个主题，内容到下一个主题开始
            end_pos = topics[idx + 1].start()
        else:
            # 最后一个主题，内容到文件末尾
            end_pos = len(content)
        
        topic_content = content[start_pos:end_pos].strip()
        
        # 移除末尾的分隔线
        topic_content = re.sub(r'\n-{6,}\s*$', '', topic_content)
        
        # 提取主题标签
        topic_tags = extract_topic_tags(topic_content)
        
        # 生成新文件名
        safe_title = sanitize_filename(topic_title)
        new_filename = f"{date_prefix}_{safe_title}.md"
        new_filepath = output_dir / new_filename
        
        # 构建新文件内容
        new_lines = []
        
        # 1. 写入更新后的元数据
        new_lines.append(f'> - **title**: {topic_title}\n')
        new_lines.append(f'> - **date**: "{original_metadata.get("date", "")}"\n')
        
        # 处理标签：合并原始tags和主题标签
        if topic_tags:
            new_lines.append(f'> - **tags**: [{topic_tags}]\n')
        else:
            new_lines.append(f'> - **tags**: {original_metadata.get("tags", "[]")}\n')
        
        new_lines.append(f'> - **categories**: {original_metadata.get("categories", "[]")}\n')
        
        # 生成新的summary
        new_summary = topic_title
        new_lines.append(f'> - **summary**: {new_summary}\n')
        
        new_lines.append('\n')
        
        # 2. 写入主题内容（移除 # 主题 N：前缀，直接从二级标题开始）
        # 移除第一行的 "# 主题 N：标题"
        topic_content_without_header = TOPIC_HEADER_RE.sub('', topic_content, count=1)
        
        # 添加一级标题
        new_lines.append(f'# {topic_title}\n\n')
        new_lines.append(topic_content_without_header)
        
        new_content = ''.join(new_lines)
        
        # 预览或写入
        if dry_run:
            print(f"\n{'='*60}")
            print(f"主题 {topic_num}: {topic_title}")
            print(f"目标文件: {new_filename}")
            print(f"标签: {topic_tags if topic_tags else '(未找到标签)'}")
            print(f"内容预览（前300字符）:")
            print(new_content[:300].replace('\n', '\n  '))
            print(f"...")
        else:
            with open(new_filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Created file: {new_filename}")
            created_files.append(new_filepath)
    
    return created_files


def main():
    parser = argparse.ArgumentParser(
        description='拆分多主题会话文档为独立文件',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('input_file', type=Path, help='输入的markdown文件路径')
    parser.add_argument('--output-dir', type=Path, help='输出目录（默认：在输入文件同级创建split_output）')
    parser.add_argument('--dry-run', action='store_true', help='预览拆分结果，不实际创建文件')
    
    args = parser.parse_args()
    
    # 检查输入文件
    if not args.input_file.exists():
        print(f"错误：文件不存在: {args.input_file}")
        return 1
    
    if not args.input_file.suffix == '.md':
        print(f"警告：文件不是markdown格式: {args.input_file}")
    
    print(f"处理文件: {args.input_file}")
    print(f"预览模式: {'是' if args.dry_run else '否'}")
    print()
    
    # 执行拆分
    created_files = split_document(args.input_file, args.output_dir, args.dry_run)
    
    # 输出结果
    if args.dry_run:
        print(f"\n{'='*60}")
        print("这是预览模式，未实际创建文件")
        print("移除 --dry-run 参数以执行实际拆分")
    else:
        print(f"\n完成！共创建 {len(created_files)} 个文件")
        if created_files:
            print(f"输出目录: {created_files[0].parent}")
    
    return 0


if __name__ == '__main__':
    exit(main())
