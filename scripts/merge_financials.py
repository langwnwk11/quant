#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
脚本名称: merge_financials.py
功能描述: 
    1. 自动遍历处理 GitHub Actions 收集到的个股分散 Parquet 文件。
    2. 将零散文件按照财报类别（balance, cash, profit）以及运行模式（报告期/单季度）融合成对应的大 Parquet 文件。
    3. 动态维护全量元数据配置的 README.md，确保 Hugging Face 预览正常。
    4. 自动清理个股零散文件夹，保持仓库纯净。
"""

import os
import sys
import shutil
import pandas as pd

def process_financial_data(base_dir, by_report_suffix):
    """
    遍历并合并指定目录下的财务数据 Parquet 文件
    :param base_dir: 基础目标文件夹，通常为 'final_output'
    :param by_report_suffix: 文件名后缀，'_report' 或 '_quarter'
    """
    categories = ["balance", "cash", "profit"]
    
    for cat in categories:
        cat_path = os.path.join(base_dir, cat)
        
        # 稳健性检查：如果在单季度模式下，balance 目录本身就不存在，则优雅跳过
        if not os.path.exists(cat_path):
            print(f"⚠️ 跳过分类 [{cat}]: 目录未找到（这在单季度模式下是正常的）。")
            continue
        
        # 1. 搜集当前分类目录下所有的个股 Parquet 文件
        files = [os.path.join(cat_path, f) for f in os.listdir(cat_path) if f.endswith('.parquet')]
        
        if files:
            print(f"🔄 正在合并 [{cat}] 目录下的 {len(files)} 个个股文件...")
            df_list = []
            
            # 逐个读取文件并加入列表，包含异常捕获，防止极个别损坏的文件导致整体崩溃
            for f in files:
                try:
                    df = pd.read_parquet(f)
                    if not df.empty:
                        df_list.append(df)
                except Exception as e:
                    print(f"❌ 读取个股文件失败 {f}: {e}，该股票已跳过。")

            # 如果成功读取到了有效的数据帧，执行 Pandas 的高性能 Concat 追加合并
            if df_list:
                merged_df = pd.concat(df_list, ignore_index=True)
                
                # 【核心区分命名】：根据模式动态生成大表文件名，例如 profit_report.parquet 或 profit_quarter.parquet
                output_file_name = f"{cat}{by_report_suffix}.parquet"
                output_file_path = os.path.join(base_dir, output_file_name)
                
                # 输出保存为全量大表
                merged_df.to_parquet(output_file_path, index=False)
                print(f"✅ 成功创建该模式财务大表: {output_file_path}")
            else:
                print(f"⚠️ 分类 [{cat}] 下未提取到有效数据。")
        else:
            print(f"⚠️ 分类 [{cat}] 文件夹为空，没有找到任何 .parquet 文件。")
        
        # 2. 彻底清理原始个股零散文件夹
        try:
            shutil.rmtree(cat_path)
            print(f"🧹 已成功清理个股零散缓存目录: {cat_path}")
        except Exception as e:
            print(f"⚠️ 清理个股临时目录失败 {cat_path}: {e}")

def update_readme_configs(target_dir):
    """
    写入极简版 README.md，仅包含 HF 必需的元数据和文件资产清单。
    """
    content = """---
configs:
- config_name: balance_report
  data_files: "balance_report.parquet"
- config_name: profit_report
  data_files: "profit_report.parquet"
- config_name: cash_report
  data_files: "cash_report.parquet"
- config_name: profit_quarter
  data_files: "profit_quarter.parquet"
- config_name: cash_quarter
  data_files: "cash_quarter.parquet"
---

# Stock Financial Dataset

### 📂 Dataset Files

* **`balance_report.parquet`** (按报告期 - 资产负债表)
* **`profit_report.parquet`** (按报告期 - 利润表)
* **`cash_report.parquet`** (按报告期 - 现金流量表)
* **`profit_quarter.parquet`** (按单季度 - 利润表)
* **`cash_quarter.parquet`** (按单季度 - 现金流量表)
"""
    readme_path = os.path.join(target_dir, "README.md")
    try:
        with open(readme_path, "w", encoding="utf-8") as f:
            f.write(content)
        print(f"📝 极简版 README.md 部署成功: {readme_path}")
    except Exception as e:
        print(f"❌ 写入 README.md 失败: {e}")

if __name__ == "__main__":
    # 从命令行获取目标目录（Action 中我们传入的是 'final_output'）
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "finalized-financial-data"

    # 从命令行获取 by_report 变量参数（'1' 代表报告期，'0' 代表单季度）
    by_report_raw = sys.argv[2] if len(sys.argv) > 2 else "1"

    # 建立输入值与文件名后缀的映射关系
    by_report_suffix = "_report" if by_report_raw == "1" else "_quarter"

    print("=" * 60)
    print(f"🚀 启动大表合并内核流程...")
    print(f"   📂 目标操纵目录: {target_dir}")
    print(f"   🎨 数据解析模式: {'[报告期 (Report Mode)]' if by_report_raw == '1' else '[单季度 (Quarter Mode)]'}")
    print(f"   🏷️ 最终大表后缀: {by_report_suffix}")
    print("=" * 60)

    # 执行主程序逻辑
    process_financial_data(target_dir, by_report_suffix)
    update_readme_configs(target_dir)

    print("=" * 60)
    print("🎉 合并完成，本地缓存已清理。")
    print("=" * 60)