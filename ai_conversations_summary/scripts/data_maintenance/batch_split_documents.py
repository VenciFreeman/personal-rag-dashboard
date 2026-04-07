"""
批量拆分多主题文档

遍历指定目录，自动拆分所有包含多主题的文档

用法：
    python batch_split_documents.py <documents_dir> [--dry-run] [--move-originals]

示例：
    python batch_split_documents.py ../documents/science/ --dry-run
    python batch_split_documents.py ../documents/ --move-originals
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

_SCRIPTS_DIR = Path(__file__).resolve().parents[1]
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from split_multi_topic_documents import split_document
import shutil


def batch_split(
    root_dir: Path,
    output_dir: Optional[Path] = None,
    dry_run: bool = False,
    move_originals: bool = False,
    recursive: bool = True
) -> dict:
    """批量拆分目录中的多主题文档"""

    def _empty_stats(total: int = 0, *, error: bool = False) -> dict:
        stats = {
            "total": total,
            "split": 0,
            "single_topic": 0,
            "errors": 0,
        }
        if error:
            stats["error"] = True
        return stats
    
    if not root_dir.exists():
        print(f"错误：目录不存在: {root_dir}")
        return _empty_stats(error=True)
    
    # 查找所有markdown文件
    if recursive:
        md_files = list(root_dir.rglob("*.md"))
    else:
        md_files = list(root_dir.glob("*.md"))
    
    if not md_files:
        print(f"未找到markdown文件: {root_dir}")
        return _empty_stats(total=0)
    
    print(f"找到 {len(md_files)} 个markdown文件")
    print(f"{'='*60}\n")
    
    stats = _empty_stats(total=len(md_files))
    
    processed_files = []
    
    for md_file in md_files:
        # 跳过已拆分的文件（在split_output目录中）
        if "split_output" in str(md_file):
            continue
        
        print(f"处理: {md_file.relative_to(root_dir)}")
        
        try:
            # 尝试拆分
            created_files = split_document(md_file, output_dir=output_dir, dry_run=dry_run)
            
            if not created_files:
                stats["single_topic"] += 1
                print("  → 跳过（单主题或无主题结构）\n")
            else:
                stats["split"] += 1
                processed_files.append((md_file, created_files))
                print(f"  → 拆分成功！生成 {len(created_files)} 个文件\n")
                
        except Exception as e:
            stats["errors"] += 1
            print(f"  → 错误: {e}\n")
            continue
    
    # 移动原始文件（如果指定）
    if move_originals and not dry_run and processed_files:
        print(f"\n{'='*60}")
        print("移动原始文件到归档目录...")
        
        for original_file, _ in processed_files:
            archive_dir = original_file.parent / "archived_multi_topic"
            archive_dir.mkdir(parents=True, exist_ok=True)
            
            dest_path = archive_dir / original_file.name
            shutil.move(str(original_file), str(dest_path))
            print(f"  移动: {original_file.name} → archived_multi_topic/")
    
    return stats


def main():
    parser = argparse.ArgumentParser(
        description='批量拆分多主题会话文档',
        formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument('root_dir', type=Path, help='文档根目录')
    parser.add_argument('--output-dir', type=Path, help='输出目录（默认：输入目录下的split_output）')
    parser.add_argument('--dry-run', action='store_true', help='预览模式，不实际创建文件')
    parser.add_argument('--move-originals', action='store_true', 
                       help='将已拆分的原始文件移动到archived_multi_topic目录')
    parser.add_argument('--no-recursive', action='store_true', 
                       help='不递归搜索子目录，只处理根目录下的文件')
    
    args = parser.parse_args()
    
    print(f"批量拆分文档")
    print(f"根目录: {args.root_dir}")
    if args.output_dir:
        print(f"输出目录: {args.output_dir}")
    print(f"预览模式: {'是' if args.dry_run else '否'}")
    print(f"移动原始文件: {'是' if args.move_originals else '否'}")
    print(f"递归搜索: {'是' if not args.no_recursive else '否'}")
    print(f"{'='*60}\n")
    
    stats = batch_split(
        args.root_dir,
        output_dir=args.output_dir,
        dry_run=args.dry_run,
        move_originals=args.move_originals,
        recursive=not args.no_recursive
    )
    
    if "error" in stats:
        return 1
    
    # 输出统计
    print(f"\n{'='*60}")
    print("批量处理完成！")
    print(f"  总文件数: {stats['total']}")
    print(f"  成功拆分: {stats['split']}")
    print(f"  单主题跳过: {stats['single_topic']}")
    print(f"  错误: {stats['errors']}")
    
    if args.dry_run:
        print(f"\n这是预览模式，未实际创建文件")
        print("移除 --dry-run 参数以执行实际拆分")
    
    return 0


if __name__ == '__main__':
    exit(main())
