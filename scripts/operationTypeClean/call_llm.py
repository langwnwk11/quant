import sys
import asyncio
import logging
import os
from typing import Dict, List, Tuple
import pandas as pd
from githubmodel import GitHubLLMClient
from pathlib import Path
# 假设 GitHubLLMClient 已在当前目录的 client.py 中定义并实现
# from client import GitHubLLMClient
ROOT_DIR = Path(__file__).parent.parent.parent
OUTPUT_BASE = ROOT_DIR / "output"
DATA_BASE =ROOT_DIR / "data"/"baseData"
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
    company_file_path: str, total_group: int
) -> List[List[str]]:
    """从公司数据中提取所有独立行业代码，并均匀分配到指定总组数中。"""
    if not os.path.exists(company_file_path):
        raise FileNotFoundError(f"未找到公司 Parquet 文件: {company_file_path}")

    # 仅读取“所属行业代码”列以节省内存
    df_industry = pd.read_parquet(company_file_path, columns=["所属行业代码"])
    unique_industries = sorted(df_industry["所属行业代码"].dropna().unique().tolist())

    total_industries = len(unique_industries)
    logger.info(f"从数据中清洗出 {total_industries} 个唯一行业代码。")

    if total_industries == 0:
        return [[] for _ in range(total_group)]

    # 核心算法：确保全覆盖的分组切片
    groups = []
    base_size = total_industries // total_group
    remainder = total_industries % total_group

    start_idx = 0
    for i in range(total_group):
        # 将余数均匀分配给前几组
        current_size = base_size + (1 if i < remainder else 0)
        end_idx = start_idx + current_size
        groups.append(unique_industries[start_idx:end_idx])
        start_idx = end_idx

    return groups


# =====================================================================
# 第二步：单行业数据组装器
# =====================================================================
def prepare_industry_companies(
    company_file_path: str, industry_code: str
) -> Tuple[str, str]:
    """提取特定行业的所有公司，拼装为指定文本格式，并获取对应的申万3级名称。"""
    df = pd.read_parquet(company_file_path)

    # 筛选出属于当前行业的记录
    df_filtered = df[df["所属行业代码"] == industry_code]
    if df_filtered.empty:
        return "", ""

    # 获取行业对应的申万3级名称（取第一行的名称即可）
    focus_name = str(df_filtered["申万3级"].iloc[0])

    # 组装格式: 公司名称,主营构成
    # 这里的“股票简称”对应输出格式里的“公司名称”
    lines = []
    for _, row in df_filtered.iterrows():
        brief = str(row["股票简称"]).strip()
        main_business = str(row["主营构成"]).strip().replace("\n", " ")
        lines.append(f"{brief},{main_business}")

    companies_text = "\n".join(lines)
    return focus_name, companies_text


# =====================================================================
# 第三步：候选池构建器
# =====================================================================
def load_candidate_pool(pool_file_path: str) -> List[str]:
    """从申万行业维表 Parquet 中提取所有标准的行业名称列表。"""
    if not os.path.exists(pool_file_path):
        raise FileNotFoundError(f"未找到行业池 Parquet 文件: {pool_file_path}")

    df_pool = pd.read_parquet(pool_file_path, columns=["行业名称"])
    pool_list = df_pool["行业名称"].dropna().unique().tolist()
    return pool_list


# =====================================================================
# 第四步：提示词工厂与核心业务调度
# =====================================================================
def build_system_prompt(pool: List[str]) -> str:
    """利用静态行业池填充构建全局唯一的 System Prompt"""
    pool_str = ",".join(pool)
    system_template = f"""# Role
你是一位精通中国A股市场、熟悉申万行业分类标准（2021版）的高级证券数据分析师。你的核心任务是将上市公司的“主营构成（业务描述）”精准映射到法定的申万行业分类中。

# Context
你目前正在处理特定申万分类的数据清洗工作。为了保持行业划分的严密性，你必须在给定的【候选申万分类池】中进行选择。
【候选申万分类池】如下：
{pool_str}

# Task & Workflow
请逐行阅读【待处理公司数据】。针对每一行数据：
1. 分析该公司的“主营构成”业务特征。
2. 从【候选申万分类池】中，寻找与该主营构成最匹配、最相关的“申万分类名称”。
3. 特别注意：如果该公司的主要业务与【当前聚焦分类】高度契合，请优先考虑是否映射到【当前聚焦分类】。

# Rules & Constraints (⚠️ 极其严格)
- 【零胡编乱造】：你输出的映射结果分类，必须100%存在于【候选申万分类池】中。绝不允许自己创造、缩写或修改任何行业名称。
- 【唯一映射】：每行公司数据只能映射到一个最核心的申万分类，不允许返回多个分类。
- 【无法映射处理】：如果某家公司的主营构成过于模糊，或没有任何依据能放入【候选申万分类池】，请将其统一映射为 "无法匹配"。
- 【严禁解释】：你的回答中只能包含指定格式的最终结果，绝对不要包含任何“我认为...”、“因为...”等解释性文字、分析过程或导语。

# Output Format
请严格按照以下 CSV 格式输出结果（不要包含 Markdown 代码块标记 ```）：
公司名称,主营构成,映射申万分类

示例输出：
万丰奥威,铝合金轮毂及飞行汽车,汽车零部件
泰格医药,临床研究服务,医药生物"""
    return system_template


def build_user_message(focus: str, companies: str) -> str:
    """动态生成每次请求变动的 User Content"""
    return f"""# Inputs (Variables)
- 【当前聚焦分类】：{focus}
- 【待处理公司数据】：
{companies}"""

def split_companies_text(companies_text: str, chunk_size: int = 30) -> List[str]:
    """
    【核心工具】将超长的公司文本按行数切分成多个小块（Chunks）
    每块最多包含 chunk_size 家公司，防止 Token 溢出并提高映射准确率
    """
    if not companies_text.strip():
        return []
    
    lines = companies_text.split("\n")
    chunks = []
    
    for i in range(0, len(lines), chunk_size):
        chunk_lines = lines[i:i + chunk_size]
        chunks.append("\n".join(chunk_lines))
        
    return chunks

async def process_industry_task(
    client: "GitHubLLMClient",
    company_file: str,
    industry_code: str,
    delay: float = 3.0,
    max_companies_per_request: int = 30  # 每批小模型处理的最佳体量
) -> str:
    """
    单个行业处理的最小单元：自动识别超长文本并分批请求 LLM，最后合并输出。
    """
    focus_name, companies_text = prepare_industry_companies(company_file, industry_code)

    if not companies_text:
        logger.warning(f"行业 {industry_code} 下无有效公司数据，跳过。")
        return ""

    # 【核心改动】将长文本切分为安全片区
    company_chunks = split_companies_text(companies_text, chunk_size=max_companies_per_request)
    num_chunks = len(company_chunks)
    
    if num_chunks > 1:
        logger.info(f"行业 [{industry_code} -> {focus_name}] 公司过多，已自动拆分为 {num_chunks} 个批次执行。")

    industry_responses = []

    # 依次处理切片（这里采用串行批处理，对 GitHub 免费限流最友好）
    for idx, chunk in enumerate(company_chunks):
        if num_chunks > 1:
            logger.info(f"  正在发送 [{focus_name}] 的第 ({idx + 1}/{num_chunks}) 批公司数据...")
            
        # 组装当前分片的 User Content
        user_msg = build_user_message(focus=focus_name, companies=chunk)

        # 调用已绑定固定的 System Prompt 客户端
        response = await client.chat(user_message=user_msg, temperature=0.0)
        
        if response:
            industry_responses.append(response.strip())
            
        # 每次请求完，强制冷却，保护 GitHub Action 的 Quota 不被封禁
        await asyncio.sleep(delay)

    # 将多批次返回的 CSV 文本行合并为一个大字符串返回
    return "\n".join(industry_responses)


# =====================================================================
# 主执行流控制
# =====================================================================
async def run_pipeline(
    company_parquet: str, pool_parquet: str, group_index: int, total_group: int
):
    # 1. 行业代码整体分组
    all_groups = get_industry_groups(company_parquet, total_group)

    if group_index < 0 or group_index >= len(all_groups):
        raise IndexError(
            f"传入的 group_index ({group_index}) 超出有效索引范围 (0~{total_group-1})"
        )

    # 2. 锁定当前 Action 实例要处理的特定行业组
    target_group = all_groups[group_index]
    logger.info(
        f"当前 Action 实例激活任务：组索引 {group_index}/{total_group}，共需处理 {len(target_group)} 个行业"
    )

    if not target_group:
        logger.info("当前组分配到的行业代码为空，程序退出。")
        return

    # 3. 加载行业维表并初始化【一次性加载】的 System Prompt
    candidate_pool = load_candidate_pool(pool_parquet)
    system_prompt_text = build_system_prompt(candidate_pool)

    # 4. 初始化具备并发控制及重试机制的 LLM 客户端
    # 注意：GitHub 免费 Token 的限流较严，此处降低 max_concurrent_requests 为 1 或 2，配合 delay 串行间歇最稳妥
    llm_client = GitHubLLMClient(
        model_name="gpt-4o", max_concurrent_requests=1, max_retries=5
    )
    llm_client.set_system_prompt(system_prompt_text)

    group_results = []

    try:
        # 5. 循环处理组内所有行业
        for industry_code in target_group:
            try:
                # 传入 delay=3.0（每请求一次歇 3 秒），可根据 GitHub 真实模型的 RPM 动态调整
                res = await process_industry_task(
                    llm_client, company_parquet, industry_code, delay=3.0
                )
                if res:
                    group_results.append(res.strip())
            except Exception as e:
                logger.error(f"行业 {industry_code} 处理期间发生异常: {e}，继续处理下一家。")

        # 6. 完成当前组内所有数据清洗后，合并去重并持久化保存到一个文件
        if group_results:
            output_filename = f"mapping_result_group_{group_index}.csv"

            # 过滤掉大模型可能吐出的 CSV 头部标题，保持最终合并结果干净
            combined_lines = []
            for res_block in group_results:
                for line in res_block.split("\n"):
                    if "公司名称,主营构成" in line or not line.strip():
                        continue
                    combined_lines.append(line.strip())

            # 写入文件
            with open(
                output_filename, "w", encoding="utf-8"
            ) as f:
                f.write("公司名称,主营构成,映射申万分类\n")
                f.write("\n".join(combined_lines))

            logger.info(
                f"🎉 组 [{group_index}] 处理完成！结果已成功持久化至: {output_filename}"
            )
        else:
            logger.warning(f"组 [{group_index}] 未收集到任何有效映射数据。")

    finally:
        await llm_client.close()



# 启动入口示例
if __name__ == "__main__":
    # 在 GitHub Action 中，这 4 个变量可以通过 sys.argv 或 环境变量 获取
    # 这里声明变量结构
    COMPANY_PATH = DATA_BASE/"operation_llm_source.parquet"
    POOL_PATH = DATA_BASE/"sw3_info.parquet"
    GROUP_IDX = int(os.environ.get("STRATEGY_GROUP_IDX", 0))
    TOTAL_GRP = int(os.environ.get("STRATEGY_TOTAL_GRP", 10))
   
    asyncio.run(
        # run_pipeline(COMPANY_PATH, POOL_PATH, int(sys.argv[2]), int(sys.argv[3]))
        run_pipeline(COMPANY_PATH, POOL_PATH, 0, 20)
    )