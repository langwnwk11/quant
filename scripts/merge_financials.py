import os
import sys
import shutil
import pandas as pd

def process_financial_data(base_dir):
    categories = ["balance", "cash", "profit"]
    generated_configs = [] # 用于记录实际成功生成的大 Parquet 文件
    
    for cat in categories:
        cat_path = os.path.join(base_dir, cat)
        if not os.path.exists(cat_path):
            print(f"⚠️ 跳过 {cat}: 目录未找到（可能由于运行了单季度模式）。")
            continue
        
        # 1. 搜集并合并 Parquet 文件
        files = [os.path.join(cat_path, f) for f in os.listdir(cat_path) if f.endswith('.parquet')]
        if files:
            print(f"🔄 正在合并 {cat} 目录下的 {len(files)} 个文件...")
            df_list = []
            for f in files:
                try:
                    df = pd.read_parquet(f)
                    if not df.empty:
                        df_list.append(df)
                except Exception as e:
                    print(f"❌ 读取文件失败 {f}: {e}，已跳过")

            if df_list:
                # 采用 concat 方式合并
                merged_df = pd.concat(df_list, ignore_index=True)
                
                # 输出到 base_dir 下
                output_file_name = f"{cat}.parquet"
                output_file_path = os.path.join(base_dir, output_file_name)
                merged_df.to_parquet(output_file_path, index=False)
                print(f"✅ 成功创建大表: {output_file_path}")
                
                # 记录成功生成的配置，用于稍后动态生成 README.md
                generated_configs.append(cat)
        
        # 2. 彻底清理原始个股文件夹（不打无意义的 zip，保持 Hugging Face 数据纯净）
        try:
            shutil.rmtree(cat_path)
            print(f"🧹 已清理原始零散文件夹: {cat_path}")
        except Exception as e:
            print(f"⚠️ 清理文件夹失败 {cat_path}: {e}")

    return generated_configs

def create_dynamic_readme(target_dir, valid_configs):
    """根据实际生成的文件，动态编写 Hugging Face 的 yaml 抬头"""
    if not valid_configs:
        print("⚠️ 没有生成任何有效的 Parquet 文件，跳过创建 README.md")
        return

    content_lines = ["---", "configs:"]
    for config in valid_configs:
        content_lines.append(f"- config_name: {config}")
        content_lines.append(f"  data_files: \"{config}.parquet\"")
    content_lines.append("---")
    content_lines.append("\n# Stock Financial Dataset")
    content_lines.append(f"\n此数据集由 GitHub Actions 自动更新。当前包含的报表类型：{', '.join(valid_configs)}。")
    
    readme_path = os.path.join(target_dir, "README.md")
    with open(readme_path, "w", encoding="utf-8") as f:
        f.write("\n".join(content_lines))
    print(f"📝 动态创建 README.md 成功: {readme_path}")

if __name__ == "__main__":
    # 接收来自 Action 的传参（在融合版 yaml 里我们传了 'final_output'）
    target_dir = sys.argv[1] if len(sys.argv) > 1 else "finalized-financial-data"
    
    print(f"🚀 开始处理目录: {target_dir}")
    valid_configs = process_financial_data(target_dir)
    create_dynamic_readme(target_dir, valid_configs)
    print("🎉 财务数据合并整合流程全部完成！")