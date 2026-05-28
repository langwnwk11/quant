import os
import asyncio
import logging
import sys
from typing import List, Dict, Any, Optional
from openai import AsyncOpenAI, OpenAIError, RateLimitError
from tenacity import retry, stop_after_attempt, wait_random_exponential, retry_if_exception_type

# 配置日志
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class GitHubLLMClient:
    def __init__(
        self, 
        model_name: str = "gpt-4o", 
        max_concurrent_requests: int = 1, # 💡 额度再大，并发也死锁为 1，确保串行最稳
        max_retries: int = 5
    ):
        """
        初始化 GitHub LLM 客户端（高固性防刷安全版）
        """
        self.base_url = "https://models.inference.ai.azure.com"
        self.token = os.environ.get("MYGITHUB_TOKEN")
        
        if not self.token:
            raise ValueError("未检测到环境变量 'MYGITHUB_TOKEN'，请先设置后再运行。")
            
        self.model_name = model_name
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.max_retries = max_retries
        self._system_prompt: Optional[str] = None
        
        self.client = AsyncOpenAI(
            base_url=self.base_url,
            api_key=self.token,
        )

    def set_system_prompt(self, system_prompt: str) -> None:
        """设置全局 System Prompt"""
        if not system_prompt.strip():
            logger.warning("设置的 System Prompt 为空。")
        self._system_prompt = system_prompt
        logger.info("System Prompt 设置成功！")

    def _get_retry_decorator(self):
        """
        构建针对网络网络错误的重试装饰器（排除硬性限流熔断）
        """
        def is_transient_error(exception):
            """💡 核心风控：判断是否为可恢复的临时错误"""
            if isinstance(exception, RateLimitError):
                err_msg = str(exception).lower()
                # 如果触发了单日用户硬性限制，或者明确要求等待几千秒，属于硬限流，绝不重试！
                if "userbymodelbyday" in err_msg or "exceeded" in err_msg or "wait" in err_msg:
                    logger.error("🚨 监测到 GitHub 账号级别硬限流封禁，重试机制主动放弃，触发熔断。")
                    return False
            # 其他属于 OpenAIError 的网络波动抖动，允许重试
            return isinstance(exception, OpenAIError)

        return retry(
            reraise=True,
            stop=stop_after_attempt(self.max_retries),
            wait=wait_random_exponential(min=5, max=60), # 最少等 5 秒再重试
            retry=is_transient_error,
            before_sleep=lambda retry_state: logger.warning(
                f"⚠️ 触发瞬时网络异常，正在进行第 {retry_state.attempt_number} 次重试... "
                f"异常原因: {retry_state.outcome.exception()}"
            )
        )

    async def chat(
        self, 
        user_message: str, 
        history: Optional[List[Dict[str, str]]] = None,
        temperature: float = 0.0, # 💡 证券清洗映射任务，温度坚决锁死为 0.0
        max_tokens: Optional[int] = None
    ) -> str:
        """发送普通提问（自动组装内部 system_prompt）"""
        messages = []
        if self._system_prompt:
            messages.append({"role": "system", "content": self._system_prompt})
        if history:
            messages.extend(history)
        messages.append({"role": "user", "content": user_message})

        return await self.chat_with_messages(messages=messages, temperature=temperature, max_tokens=max_tokens)

    async def chat_with_messages(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        max_tokens: Optional[int] = None
    ) -> str:
        """💡 补齐缺失的核心接口：直接发送由 LangChain 组装好的标准 Messages 列表"""
        
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

        # 严格并发受控
        async with self.semaphore:
            try:
                return await _execute_with_retry()
            except Exception as e:
                # 将异常向上抛出，供外部业务脚本捕捉并决定是否 sys.exit(1)
                raise e

    async def close(self):
        """关闭客户端连接"""
        await self.client.close()