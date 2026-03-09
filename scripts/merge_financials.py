# 合并github 每个group中的数据，按照财报类型统一打包
import pandas as pd
import os
import shutil
import sys

def process_financial_data(base_dir):
    categories = ["balance", "cash", "profit"]
    
    for cat in categories:
        cat_path = os.path.join(base_dir, cat)
        if not os.path.exists(cat_path):
            print(f"Skipping {cat}: Directory not found.")
            continue
        
        # 1. 合并 Parquet 文件
        files = [os.path.join(cat_path, f) for f in os.listdir(cat_path) if f.endswith('.parquet')]
        if files:
            print(f"Merging {len(files)} files in {cat}...")
            # 采用 concat 方式合并
            df_list = [pd.read_parquet(f) for f in files]
            merged_df = pd.concat(df_list, ignore_index=True)
            # 输出到 base_dir 下
            output_file = os.path.join(base_dir, f"{cat}.parquet")
            merged_df.to_parquet(output_file, index=False)
            print(f"Created: {output_file}")
        
        # 2. 压缩原文件夹 (目标文件名: base_dir/cat.zip)
        zip_output_path = os.path.join(base_dir, cat) # shutil 会自动补 .zip
        shutil.make_archive(zip_output_path, 'zip', cat_path)
        print(f"Created: {zip_output_path}.zip")
        
        # 3. 删除原始文件夹
        shutil.rmtree(cat_path)
        print(f"Cleaned up directory: {cat_path}")

# 在你的 merg_financials.py 末尾添加
def create_readme(target_dir):
    content = """---
configs:
- config_name: balance
  data_files: "balance.parquet"
- config_name: cash
  data_files: "cash.parquet"
- config_name: profit
  data_files: "profit.parquet"
---
"""
    with open(os.path.join(target_dir, "README.md"), "w") as f:
        f.write(content)

if __name__ == "__main__":
    # 默认处理当前目录下的 finalized-financial-data
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "finalized-financial-data"
    process_financial_data(target_dir)
    create_readme(target_dir)