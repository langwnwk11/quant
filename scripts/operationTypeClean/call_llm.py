import sys
import asyncio
import logging
import os
import csv
import io
import time
from typing import Dict, List, Tuple
import pandas as pd
from githubmodel import GitHubLLMClient
from pathlib import Path

ROOT_DIR = Path(__file__).parent.parent.parent
OUTPUT_BASE = ROOT_DIR / "output"
DATA_BASE = ROOT_DIR / "data" / "baseData"
MAX_REPAIR_ATTEMPTS = 5

# 配置日志
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# =====================================================================
# 第一步：行业代码分组提取器
# =====================================================================
def get_industry_groups(
    company_file_path: Path, total_group: int
) -> List[List[str]]:
    """从公司数据中提取所有独立行业代码，并均匀分配到指定总组数中。"""
    if not company_file_path.exists():
        raise FileNotFoundError(f"未找到公司 Parquet 文件: {company_file_path}")

    # 仅读取“所属行业代码”列以节省内存
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
# 第二步：单行业数据组装器
# =====================================================================
def prepare_industry_companies(
    company_file_path: Path, industry_code: str
) -> Tuple[str, str]:
    """提取特定行业的所有公司，安全拼装为标准 CSV 文本格式。"""
    df = pd.read_parquet(company_file_path)

    df_filtered = df[df["所属行业代码"] == industry_code]
    if df_filtered.empty:
        return "", ""

    focus_name = str(df_filtered["申万3级"].iloc[0])

    # 💡 安全组装: 使用 StringIO 和 csv.writer 自动处理文本内包含的逗号、引号等冲突
    output = io.StringIO()
    writer = csv.writer(output)
    
    for _, row in df_filtered.iterrows():
        brief = str(row["股票简称"]).strip()
        main_business = str(row["主营构成"]).strip().replace("\n", " ")
        writer.writerow([brief, main_business])

    companies_text = output.getvalue().strip()
    return focus_name, companies_text


# =====================================================================
# 第三步：候选池构建器
# =====================================================================
def load_candidate_pool(pool_file_path: Path) -> List[str]:
    """从申万行业维表 Parquet 中提取所有标准的行业名称列表。"""
    if not pool_file_path.exists():
        raise FileNotFoundError(f"未找到行业池 Parquet 文件: {pool_file_path}")

    df_pool = pd.read_parquet(pool_file_path, columns=["行业名称"])
    return df_pool["行业名称"].dropna().unique().tolist()


# =====================================================================
# 第四步：提示词工厂与数据清洗核心
# =====================================================================
def build_system_prompt(pool: List[str]) -> str:
    """利用静态行业池填充构建全局唯一的 System Prompt"""
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


def split_companies_text(companies_text: str, chunk_size: int = 30) -> List[str]:
    if not companies_text.strip():
        return []
    lines = companies_text.split("\n")
    return ["\n".join(lines[i:i + chunk_size]) for i in range(0, len(lines), chunk_size)]


def clean_llm_csv_response(response_text: str) -> List[str]:
    """💡 防御性清洗：剔除 LLM 偶尔固执吐出的 Markdown 标记、空白行以及重复的表头"""
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


async def process_industry_task(
    client: "GitHubLLMClient",
    company_file: Path,
    industry_code: str,
    delay: float = 6.0,  # 💡 建议设为 6.0 秒，保障 6万次大额度下不触发瞬时频控
    max_companies_per_request: int = 30
) -> List[str]:
    """单个行业处理的最小单元：自动识别超长文本并分批请求 LLM，返回纯数据行列表。"""
    focus_name, companies_text = prepare_industry_companies(company_file, industry_code)

    if not companies_text:
        logger.warning(f"行业 {industry_code} 下无有效公司数据，跳过。")
        return []

    company_chunks = split_companies_text(companies_text, chunk_size=max_companies_per_request)
    num_chunks = len(company_chunks)
    
    if num_chunks > 1:
        logger.info(f"行业 [{industry_code} -> {focus_name}] 公司过多，已自动拆分为 {num_chunks} 个批次执行。")

    industry_lines = []

    for idx, chunk in enumerate(company_chunks):
        if num_chunks > 1:
            logger.info(f"  正在发送 [{focus_name}] 的第 ({idx + 1}/{num_chunks}) 批公司数据...")
            
        user_msg = build_user_message(focus=focus_name, companies=chunk)

        # 核心请求
        response = await client.chat(user_message=user_msg, temperature=0.0)
        
        if response:
            # 清洗并收集数据行
            cleaned_chunks = clean_llm_csv_response(response)
            industry_lines.extend(cleaned_chunks)
            
        # 强制冷却保护
        await asyncio.sleep(delay)

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
    logger.info(f"当前 Action 实例激活任务：组索引 {group_index}/{total_group}，共需处理 {len(target_group)} 个行业")

    if not target_group:
        logger.info("当前组分配到的行业代码为空，程序退出。")
        return

    candidate_pool = load_candidate_pool(pool_parquet)
    system_prompt_text = build_system_prompt(candidate_pool)

    # 初始化具备并发控制的 LLM 客户端
    llm_client = GitHubLLMClient(
        model_name="gpt-4o", max_concurrent_requests=1, max_retries=5
    )
    llm_client.set_system_prompt(system_prompt_text)

    combined_csv_lines = []

    try:
        for industry_code in target_group:
            try:
                # 💡 显式传入延迟，确保每次调用后稳健歇息 6 秒
                res_lines = await process_industry_task(
                    llm_client, company_parquet, industry_code, delay=6.0
                )
                if res_lines:
                    combined_csv_lines.extend(res_lines)
            except Exception as e:
                err_msg = str(e)
                # 💡 核心策略：如果异常中包含限流关键词，直接终止系统，让 Action 报错熔断
                if "429" in err_msg or "RateLimitReached" in err_msg or "limit exceeded" in err_msg.lower():
                    logger.error(f"🚨 触发 GitHub 瞬时频控硬拦截！错误原因: {err_msg}。正在执行紧急熔断退出...")
                    sys.exit(1)
                
                logger.error(f"行业 {industry_code} 处理期间发生非限流异常: {e}，继续处理下一家。")

        # 持久化保存
        if combined_csv_lines:
            OUTPUT_BASE.mkdir(parents=True, exist_ok=True)
            output_file = OUTPUT_BASE / f"mapping_result_group_{group_index}.csv"

            with open(output_file, "w", encoding="utf-8") as f:
                f.write("公司名称,主营构成,映射申万分类\n")
                f.write("\n".join(combined_csv_lines) + "\n")

            logger.info(f"🎉 组 [{group_index}] 处理完成！结果已成功持久化至: {output_file}")
        else:
            logger.warning(f"组 [{group_index}] 未收集到任何有效映射数据。")

    finally:
        await llm_client.close()


# 启动入口
if __name__ == "__main__":
    COMPANY_PATH = DATA_BASE / "operation_llm_source.parquet"
    POOL_PATH = DATA_BASE / "sw3_info.parquet"
    GROUP_IDX = int(os.environ.get("STRATEGY_GROUP_IDX", 0))
    TOTAL_GRP = int(os.environ.get("STRATEGY_TOTAL_GRP", 20)) # 保持与 YAML 20组一致
    
    # 💡 自适应冷启动：为了防止 Action 的 max-parallel: 1 切换过快导致上一个残余计数没清零
    # 序号越往后的组，在虚拟机启动时先原地静默一会，错开时间窗口
    if GROUP_IDX > 0:
        sleep_time = 10
        logger.info(f"⏳ 激活自适应防蜂拥机制：第 {GROUP_IDX} 组将在静默 {sleep_time} 秒后开启冷启动...")
        time.sleep(sleep_time)
   
    asyncio.run(
        run_pipeline(COMPANY_PATH, POOL_PATH, GROUP_IDX, TOTAL_GRP)
    )