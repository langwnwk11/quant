import sys
import asyncio
import logging
import os
import csv
import io
import time
from typing import Dict, List, Tuple, Set
import pandas as pd
from githubmodel import GitHubLLMClient
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent.resolve()
ROOT_DIR = SCRIPT_DIR.parent.parent 
DATA_BASE = ROOT_DIR / "data" / "baseData"

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# =====================================================================
# 第一步：行业代码分组提取器（保持原样：按行业数量严格均分）
# =====================================================================
def get_industry_groups(company_file_path: Path, total_group: int) -> List[List[str]]:
    if not company_file_path.exists():
        raise FileNotFoundError(f"未找到公司 Parquet 文件: {company_file_path}")

    df_industry = pd.read_parquet(company_file_path, columns=["所属行业代码"])
    unique_industries = sorted(df_industry["所属行业代码"].dropna().unique().tolist())
    total_industries = len(unique_industries)
    logger.info(f"从数据中清洗出 {total_industries} 个唯一行业代码。")

    if total_industries == 0:
        return [[] for _ in range(total_group)]

    groups = []
    base_size = total_industries // total_group
    remainder = total_industries % total_group

    start_idx = 0
    for i in range(total_group):
        current_size = base_size + (1 if i < remainder else 0)
        end_idx = start_idx + current_size
        groups.append(unique_industries[start_idx:end_idx])
        start_idx = end_idx
    return groups

# =====================================================================
# 🕵️‍♂️ 新增辅助步骤：读取本地已成功保存的公司，用于精确去重
# =====================================================================
def load_processed_companies(output_file: Path) -> Set[str]:
    """读取本地已经清洗出来的 CSV 文件，返回已处理的公司简称集合。"""
    processed = set()
    if not output_file.exists():
        return processed
    try:
        df = pd.read_csv(output_file, usecols=["公司名称"])
        processed = set(df["公司名称"].dropna().astype(str).tolist())
        logger.info(f"检测到本地历史记录，已加载 {len(processed)} 家已处理完成的公司，本次将自动跳过。")
    except Exception as e:
        logger.warning(f"读取历史 CSV 文件进行去重时发生异常（可能仅有表头）: {e}")
    return processed

# =====================================================================
# 第二步：单行业数据组装器（联动精准去重）
# =====================================================================
def prepare_industry_companies_v2(
    company_file_path: Path, industry_code: str, processed_set: Set[str]
) -> Tuple[str, str, int]:
    """提取特定行业未处理的公司，安全拼装为标准 CSV 文本格式。"""
    df = pd.read_parquet(company_file_path)
    df_filtered = df[df["所属行业代码"] == industry_code]
    if df_filtered.empty:
        return "", "", 0

    focus_name = str(df_filtered["申万3级"].iloc[0])
    output = io.StringIO()
    writer = csv.writer(output)
    
    valid_count = 0
    for _, row in df_filtered.iterrows():
        brief = str(row["股票简称"]).strip()
        # 💡 如果这个公司在之前的运行中已经洗过了，直接过！
        if brief in processed_set:
            continue
        
        main_business = str(row["主营构成"]).strip().replace("\n", " ")
        writer.writerow([brief, main_business])
        valid_count += 1

    companies_text = output.getvalue().strip()
    return focus_name, companies_text, valid_count

# =====================================================================
# 第三步：候选池构建器
# =====================================================================
def load_candidate_pool(pool_file_path: Path) -> List[str]:
    if not pool_file_path.exists():
        raise FileNotFoundError(f"未找到行业池 Parquet 文件: {pool_file_path}")
    df_pool = pd.read_parquet(pool_file_path, columns=["行业名称"])
    return df_pool["行业名称"].dropna().unique().tolist()

# =====================================================================
# 第四步：提示词工厂
# =====================================================================
def build_system_prompt(pool: List[str]) -> str:
    pool_str = ",".join(pool)
    return f"""# Role
你是一位精通中国A股市场、熟悉申万行业分类标准（2021版）的高级证券数据分析师。你的核心任务是将上市公司的“主营构成（业务描述）”精准映射到法定的申万行业分类中。

# Context
你必须在给定的【候选申万分类池】中进行选择。
【候选申万分类池】如下：
{pool_str}

# Task & Workflow
请逐行阅读【待处理公司数据】（每行格式为: 公司名称,主营构成）。针对每一行数据：
1. 分析该公司的“主营构成”业务特征。
2. 从【候选申万分类池】中，寻找与该主营构成最匹配、最相关的“申万分类名称”。
3. 如果该公司的主要业务与【当前聚焦分类】高度契合，请优先考虑是否映射到【当前聚焦分类】。

# Rules & Constraints (⚠️ 极其严格)
- 【零胡编乱造】：你输出的映射结果分类，必须100%存在于【候选申万分类池】中。绝不允许自己创造、缩写或修改任何行业名称。
- 【唯一映射】：每行公司数据只能映射到一个最核心的申万分类，不允许返回多个分类。
- 【无法映射处理】：如果没有任何依据能放入池中，请将其统一映射为 "无法匹配"。
- 【严禁解释】：你的回答中只能包含指定格式的最终结果，绝对不要包含任何 markdown 代码块标记（如 ``` ）、分析过程或导语。
- 【绝对全覆盖】：本次输入的待处理公司数据较多，你必须逐行分析并映射，100% 覆盖输入的每一家公司。绝对不允许漏掉、省略、或使用“等等”进行概括。输入多少行，你的 CSV 结果就必须输出多少行！

# Output Format
请严格按照以下 CSV 格式输出结果：
公司名称,主营构成,映射申万分类

万丰奥威,铝合金轮毂及飞行汽车,汽车零部件
泰格医药,临床研究服务,医药生物"""

def build_user_message(focus: str, companies: str) -> str:
    return f"""# Inputs (Variables)
- 【当前聚焦分类】：{focus}
- 【待处理公司数据】：
{companies}"""

def split_companies_text(companies_text: str, chunk_size: int) -> List[str]:
    if not companies_text.strip():
        return []
    lines = companies_text.split("\n")
    return ["\n".join(lines[i:i + chunk_size]) for i in range(0, len(lines), chunk_size)]

def clean_llm_csv_response(response_text: str) -> List[str]:
    cleaned_lines = []
    for line in response_text.split("\n"):
        line_str = line.strip()
        if (
            not line_str 
            or line_str.startswith("```") 
            or "公司名称,主营构成" in line_str
            or line_str == "映射申万分类"
        ):
            continue
        cleaned_lines.append(line_str)
    return cleaned_lines

# =====================================================================
# 🚀 改造后的单行业执行：支持动态调速防御机制
# =====================================================================
async def process_industry_task_v2(
    client: "GitHubLLMClient",
    focus_name: str,
    companies_text: str,
    total_companies: int
) -> List[str]:
    """单个行业处理：根据剩余未洗公司总数，自动计算防御性延迟，防止撞墙。"""
    
    # 💡 智能化防御策略：如果当前大行业未洗公司非常多，说明是巨型危险源，自动切成小批次、高延迟
    if total_companies > 60:
        current_chunk_size = 15
        current_delay = 15.0  # 极度温柔的速录延迟
        logger.info(f"🚨 [巨型行业警告] [{focus_name}] 剩余待清洗公司多达 {total_companies} 家！自动启动「长线避让模式」(Chunk={current_chunk_size}, Delay={current_delay}s)")
    else:
        current_chunk_size = 30
        current_delay = 6.0   # 正常组速度
        
    company_chunks = split_companies_text(companies_text, chunk_size=current_chunk_size)
    num_chunks = len(company_chunks)
    
    industry_lines = []
    for idx, chunk in enumerate(company_chunks):
        if num_chunks > 1:
            logger.info(f"   --> 正在发送 [{focus_name}] 行业的子批次 ({idx + 1}/{num_chunks})...")
            
        user_msg = build_user_message(focus=focus_name, companies=chunk)
        response = await client.chat(user_message=user_msg, temperature=0.0)
        
        if response:
            cleaned_chunks = clean_llm_csv_response(response)
            industry_lines.extend(cleaned_chunks)
            
        await asyncio.sleep(current_delay)

    return industry_lines

# =====================================================================
# 主执行流控制
# =====================================================================
async def run_pipeline(
    company_parquet: Path, pool_parquet: Path, group_index: int, total_group: int
):
    all_groups = get_industry_groups(company_parquet, total_group)

    if group_index < 0 or group_index >= len(all_groups):
        raise IndexError(f"传入的 group_index ({group_index}) 超出有效范围 (0~{total_group-1})")

    target_group = all_groups[group_index]
    logger.info(f"当前 Action 实例激活任务：组索引 {group_index}/{total_group}，共包含 {len(target_group)} 个行业。")

    if not target_group:
        logger.info("当前组分配到的行业代码为空，程序退出。")
        return

    candidate_pool = load_candidate_pool(pool_parquet)
    system_prompt_text = build_system_prompt(candidate_pool)

    llm_client = GitHubLLMClient(
        model_name="gpt-4o", max_concurrent_requests=1, max_retries=5
    )
    llm_client.set_system_prompt(system_prompt_text)

    output_file = ROOT_DIR / f"mapping_result_group_{group_index}.csv"
    
    # 初始化创建具有标准表头的文件（不存在时才建）
    if not output_file.exists():
        with open(output_file, "w", encoding="utf-8-sig") as f:
            f.write("公司名称,主营构成,映射申万分类\n")

    # 💡 核心：加载本地已经洗过的公司库，不管是哪次 Run 留下的
    processed_companies = load_processed_companies(output_file)
    has_new_data_written = False

    try:
        for industry_code in target_group:
            # 💡 传入已处理集合，只拼装未处理的公司数据
            focus_name, companies_text, needed_count = prepare_industry_companies_v2(
                company_parquet, industry_code, processed_companies
            )
            
            if needed_count == 0:
                # 如果这个行业里所有公司都在历史 CSV 里找到了，直接秒跳过！
                if focus_name:
                    logger.info(f"⏭️ 行业 [{industry_code} -> {focus_name}] 包含的公司历史上已全部映射成功，完美跳过。")
                continue

            try:
                # 激活带防御调速的流式调用
                res_lines = await process_industry_task_v2(
                    llm_client, focus_name, companies_text, needed_count
                )
                if res_lines:
                    with open(output_file, "a", encoding="utf-8-sig") as f:
                        f.write("\n".join(res_lines) + "\n")
                    has_new_data_written = True
                    logger.info(f"✅ 行业 [{focus_name}] 的 {needed_count} 家新公司清洗完毕并实时追加磁盘。")
                    
            except Exception as e:
                err_msg = str(e)
                if "429" in err_msg or "RateLimitReached" in err_msg or "limit exceeded" in err_msg.lower():
                    logger.error(f"🚨 触发 GitHub 瞬时频控硬拦截！错误原因: {err_msg}。前序数据已安全落盘，正在优雅断点熔断...")
                    await llm_client.close()
                    sys.exit(1)
                
                logger.error(f"行业 {industry_code} 处理期间发生非限流异常: {e}，继续处理下一个行业。")

        # 兜底清理哑弹
        if output_file.exists() and output_file.stat().st_size <= 45: 
            output_file.unlink()
            logger.warning(f"⚠️ 组 [{group_index}] 未收集到任何有效映射数据，已清理空文件。")
        else:
            logger.info(f"🎉 组 [{group_index}] 所有行业全部清洗完毕！完整结果锁定在: {output_file}")

    finally:
        await llm_client.close()

if __name__ == "__main__":
    COMPANY_PATH = DATA_BASE / "operation_llm_source.parquet"
    POOL_PATH = DATA_BASE / "sw3_info.parquet"
    GROUP_IDX = int(os.environ.get("STRATEGY_GROUP_IDX", 0))
    TOTAL_GRP = int(os.environ.get("STRATEGY_TOTAL_GRP", 20))
    
    if GROUP_IDX > 0:
        sleep_time = 10
        logger.info(f"⏳ 激活自适应防蜂拥机制：第 {GROUP_IDX} 组将在静默 {sleep_time} 秒后开启冷启动...")
        time.sleep(sleep_time)
   
    asyncio.run(
        run_pipeline(COMPANY_PATH, POOL_PATH, GROUP_IDX, TOTAL_GRP)
    )