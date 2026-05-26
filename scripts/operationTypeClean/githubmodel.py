import os
import asyncio
import logging
from typing import List, Dict, Any, Optional
from openai import AsyncOpenAI, OpenAIError
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type
import asyncio
# 引入 LangChain 核心的聊天提示词模板
from langchain_core.prompts import ChatPromptTemplate
# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubLLMClient:
    def __init__(
        self, 
        model_name: str = "gpt-4o", 
        max_concurrent_requests: int = 2,
        max_retries: int = 5
    ):
        """
        初始化 GitHub LLM 客户端
        """
        self.base_url = "https://models.inference.ai.azure.com"
        self.token = os.environ.get("MYGITHUB_TOKEN")
        
        if not self.token:
            raise ValueError("未检测到环境变量 'GITHUB_TOKEN'，请先设置后再运行。")
            
        self.model_name = model_name
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.max_retries = max_retries
        
        # 初始化系统提示词为空，后续通过专用接口设置
        self._system_prompt: Optional[str] = None
        
        # 初始化异步 OpenAI 客户端
        self.client = AsyncOpenAI(
            base_url=self.base_url,
            api_key=self.token,
        )

    def set_system_prompt(self, system_prompt: str) -> None:
        """
        【专用接口】设置或更新 System Prompt
        """
        if not system_prompt.strip():
            logger.warning("设置的 System Prompt 为空。")
        self._system_prompt = system_prompt
        logger.info("System Prompt 设置成功！")

    def _get_retry_decorator(self):
        """
        构建针对 429 限流和网络错误的指数退避重试装饰器
        """
        return retry(
            reraise=True,
            stop=stop_after_attempt(self.max_retries),
            wait=wait_random_exponential(min=2, max=60),
            retry=retry_if_exception_type(OpenAIError),
            before_sleep=lambda retry_state: logger.warning(
                f"触发限流或网络异常，正在进行第 {retry_state.attempt_number} 次重试... "
                f"异常原因: {retry_state.outcome.exception()}"
            )
        )

    async def chat(
        self, 
        user_message: str, 
        history: Optional[List[Dict[str, str]]] = None,
        temperature: float = 0.7, 
        max_tokens: Optional[int] = None
    ) -> str:
        """
        发送聊天请求（自动附加已设置的 system_prompt，并自带限流与重试保护）
        
        :param user_message: 用户当前的提问
        :param history: 历史对话上下文，格式为 [{"role": "user"/"assistant", "content": "..."}]
        :param temperature: 采样温度
        :param max_tokens: 最大生成 Token 数
        """
        # 1. 动态构建消息列表
        messages = []
        
        # 如果之前通过接口设置了 system_prompt，则作为第一条消息注入
        if self._system_prompt:
            messages.append({"role": "system", "content": self._system_prompt})
            
        # 如果传入了历史会话，追加到后面
        if history:
            messages.extend(history)
            
        # 最后放入当前用户的输入
        messages.append({"role": "user", "content": user_message})

        # 2. 定义内部带重试保护的请求函数
        @self._get_retry_decorator()
        async def _execute_with_retry():
            kwargs = {
                "messages": messages,
                "model": self.model_name,
                "temperature": temperature
            }
            if max_tokens:
                kwargs["max_tokens"] = max_tokens
                
            response = await self.client.chat.completions.create(**kwargs)
            return response.choices[0].message.content

        # 3. 严格通过信号量限制并发
        async with self.semaphore:
            try:
                return await _execute_with_retry()
            except Exception as e:
                logger.error(f"请求最终失败，已达最大重试次数。错误原因: {e}")
                raise e

    async def close(self):
        """关闭客户端连接"""
        await self.client.close()


# ==========================================
# 🛠️ 封装：LangChain Prompt 组装工具方法
# ==========================================
def build_securities_cleaning_prompt(pool: List[str], focus: str, companies: str) -> List[Dict[str, str]]:
    """
    【封装方法】将业务数据转化为符合 OpenAI 规范的 Messages 列表。
    如果缺少变量，LangChain 会在本地直接报错，避免浪费网络请求。
    """
    # 1. 锁死不变的业务规则
    system_template = """
# Role
你是一位精通中国A股市场、熟悉申万行业分类标准（2021版）的高级证券数据分析师。
你的核心任务是将上市公司的“主营构成（业务描述）”精准映射到法定的申万行业分类中。

# Context
你目前正在处理特定申万分类的数据清洗工作。为了保持行业划分的严密性，
你必须在给定的【候选申万分类池】中进行选择。
【候选申万分类池】如下：
{pool}


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
泰格医药,临床研究服务,医药生物
"""

    # 2. 锁死动态变量的容器格式
    user_template = """
    # Inputs (Variables)
- 【当前聚焦分类】：{focus}
- 【待处理公司数据】：
{companies}
"""

    # 3. 创建 LangChain 模板
    prompt_template = ChatPromptTemplate.from_messages([
        ("system", system_template),
        ("user", user_template)
    ])

    # 4. 渲染变量并转化为标准的 dict 数组
    # 用 "、".join(pool) 在这里直接处理清洗，让 main 函数里的输入更纯净
    prompt_value = prompt_template.invoke({
        "pool": "、".join(pool),
        "focus": focus,
        "companies": companies
    })
    
    # 转换为标准的 OpenAI 格式 [{'role': '...', 'content': '...'}, ...]
    return [{"role": msg.type, "content": msg.content} for msg in prompt_value.to_messages()]


 


# ==========================================
# 🚀 使用示例
# ==========================================
async def main():

    # 1. 初始化大模型客户端
    client = GitHubLLMClient(model_name="gpt-4o")
    
    # 2. 准备纯净的业务数据（可以轻易改为从 Excel 循环读取）
    current_pool = ["汽车零部件", "乘用车", "医药生物", "无法匹配"]
    current_focus = "汽车零部件"
    raw_companies = "万丰奥威,铝合金轮毂及飞行汽车\n泰格医药,临床研究服务"

    # 3. 【一键调用】直接调用封装方法，获取组装好的标准 Prompt
    formatted_messages = build_securities_cleaning_prompt(
        pool=current_pool,
        focus=current_focus,
        companies=raw_companies
    )
    print(formatted_messages   )
    # 4. 直接把消息喂给客户端，温度设为 0 确保严格遵循 CSV 格式
    logger.info("正在提交证券数据映射任务...")
    result = await client.chat_with_messages(messages=formatted_messages, temperature=0.0)
    
    print("\n" + "="*15 + " 最终清洗结果 " + "="*15)
    print(result)
    
    await client.close()

if __name__ == "__main__":
    # 执行异步主函数
    asyncio.run(main())