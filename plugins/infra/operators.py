"""
infra/operators.py
治理算子基类

提供统一的执行框架：
- 模板方法模式
- Context 自动管理
- 配置驱动
- 分区清理
- 异常处理
"""
import os
import yaml
from abc import ABC, abstractmethod
from typing import Optional, Dict, Any

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from plugins.domain.context import GovernanceContext
from plugins.infra.config import Config
from plugins.infra.io_strategy import IOStrategy


class BaseGovernanceOperator(BaseOperator, ABC):
    """
    治理算子基类
    
    设计模式：模板方法 (Template Method)
    
    执行流程：
    1. pre_execute()    - 初始化（恢复 Context、注入 IO 策略）
    2. execute_logic()  - 核心业务逻辑（子类实现，in-place 修改 ctx）
    3. post_execute()   - 收尾（记录日志、打印摘要）
    4. return           - 返回序列化的 Context（Airflow 自动存为 return_value）
    
    子类职责：
    - 实现 execute_logic(ctx, context) 方法
    - 根据需要显式调用 _clean_partition() 清理分区
    - 如需支持多上游，重写 _restore_context() 方法
    """
    
    # 模板字段（Airflow 会自动渲染 Jinja 模板）
    template_fields = ('config_path',)
    
    @apply_defaults
    def __init__(
        self,
        config_path: Optional[str] = None,      # YAML 配置文件路径（相对于 plugins/）
        config_dict: Optional[Dict] = None,     # 或直接传入配置字典
        upstream_task_id: Optional[str] = None, # 上游 Task ID（用于恢复 Context）
        **kwargs
    ):
        """
        初始化算子
        
        Args:
            config_path: 配置文件路径（相对于 $AIRFLOW_HOME/plugins/）
            config_dict: 配置字典（优先级高于 config_path）
            upstream_task_id: 上游 Task ID（用于从 XCom 恢复 Context）
            **kwargs: BaseOperator 的其他参数
        """
        super().__init__(**kwargs)
        self.config_path = config_path
        self.config_dict = config_dict
        self.upstream_task_id = upstream_task_id
        
        # 加载配置（在 DAG 解析时执行）
        self._config = self._load_config()
    
    # ========================================================================
    # 模板方法（Template Method）
    # ========================================================================
    
    def execute(self, context: Dict[str, Any]) -> str:
        """
        统一执行流程（Airflow 调用的入口）
        
        Args:
            context: Airflow context 字典
        
        Returns:
            序列化的 Context JSON 字符串（自动存为 return_value）
        
        Raises:
            Exception: 执行失败时抛出异常
        """
        try:
            # Step 1: 初始化阶段
            ctx = self.pre_execute(context)
            
            # Step 2: 核心业务逻辑（子类实现，in-place 修改 ctx）
            self.execute_logic(ctx, context)
            
            # Step 3: 收尾阶段
            self._post_execute_internal(ctx, context)
            
            # Step 4: 返回序列化的 Context（Airflow 标准）
            return ctx.to_json()
            
        except Exception as e:
            # 异常处理
            self.handle_error(e, context)
            raise
    
    # ========================================================================
    # 抽象方法（子类必须实现）
    # ========================================================================
    
    @abstractmethod
    def execute_logic(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        核心业务逻辑（子类实现）
        
        Args:
            ctx: 治理上下文（in-place 修改）
            context: Airflow context 字典
        
        Returns:
            None（直接修改 ctx，不返回）
        
        示例:
            def execute_logic(self, ctx, context):
                # 1. 读取配置
                config = self._config
                
                # 2. 处理数据
                df = self._process_data()
                
                # 3. 写入 Context（in-place）
                ctx.put_dataframe('result', df, stage='ENTITY')
        """
        pass
    
    # ========================================================================
    # 钩子方法（Hook Methods）
    # ========================================================================
    
    def pre_execute(self, context: Dict[str, Any]) -> GovernanceContext:
        """
        初始化阶段
        
        职责：
        1. 从上游 XCom 恢复 Context（或创建新 Context）
        2. 注入 IO 策略（验证配置一致性）
        3. 记录启动日志
        
        Args:
            context: Airflow context 字典
        
        Returns:
            GovernanceContext 对象
        """
        # 1. 恢复 Context
        ctx = self._restore_context(context)
        
        # 2. 注入 IO 策略
        self._inject_io_strategy(ctx)
        
        # 3. 记录日志
        ctx.log(f"Task [{self.task_id}] started")
        self.log.info(f"Initialized context: batch_id={ctx.batch_id}, run_date={ctx.run_date}")
        
        return ctx
    
    def _post_execute_internal(self, ctx: GovernanceContext, context: Dict[str, Any]) -> None:
        """
        收尾阶段（内部方法，避免与 Airflow 的 post_execute 冲突）
        
        职责：
        1. 记录完成日志
        2. 打印 Context 摘要
        3. 预留：发送通知等
        
        Args:
            ctx: 治理上下文
            context: Airflow context 字典
        """
        # 1. 记录日志
        ctx.log(f"Task [{self.task_id}] completed")
        
        # 2. 打印摘要
        summary = ctx.summary()
        self.log.info(f"Context Summary: {summary}")
        
        # 3. 打印详细信息（可选）
        if ctx.alerts:
            self.log.info(f"Generated {len(ctx.alerts)} alerts")
        if ctx.assets:
            self.log.info(f"Generated {len(ctx.assets)} assets")
        if ctx.rule_outputs:
            self.log.info(f"Executed {len(ctx.rule_outputs)} rules")
    
    def post_execute(self, context: Dict[str, Any], result=None) -> None:
        """
        Airflow 框架回调（兼容 Airflow 2.x）
        
        注意：
        - 此方法由 Airflow 框架自动调用，不要手动调用
        - 我们的内部收尾逻辑在 _post_execute_internal() 中
        - 这里只是为了兼容 Airflow 的调用，避免 TypeError
        
        Args:
            context: Airflow 上下文
            result: 任务执行结果（可选）
        """
        # 空实现，因为我们的收尾逻辑已在 _post_execute_internal() 中完成
        pass
    
    # ========================================================================
    # Context 管理
    # ========================================================================
    
    def _restore_context(self, context: Dict[str, Any]) -> GovernanceContext:
        """
        恢复 Context（单上游场景）
        
        策略：
        1. 优先使用显式指定的 upstream_task_id
        2. 自动从 Airflow 上游任务列表获取（单上游场景）
        3. 如果没有上游，创建新 Context（兜底逻辑）
        
        子类可重写此方法以支持多上游场景（如 Aggregator）
        
        Args:
            context: Airflow context 字典
        
        Returns:
            GovernanceContext 对象
        """
        ti = context['task_instance']
        
        # 确定上游任务 ID
        upstream_id = self.upstream_task_id
        
        # 如果没有显式指定，尝试自动获取
        if not upstream_id:
            upstream_task_ids = ti.task.upstream_task_ids
            if len(upstream_task_ids) == 1:
                upstream_id = list(upstream_task_ids)[0]
                self.log.info(f"🔍 Auto-detected upstream task: {upstream_id}")
            elif len(upstream_task_ids) > 1:
                self.log.warning(
                    f"⚠️ Multiple upstream tasks found: {upstream_task_ids}. "
                    f"Please specify 'upstream_task_id' explicitly or override '_restore_context()'."
                )
        
        # 从上游恢复
        if upstream_id:
            try:
                # 拉取上游的 return_value（Airflow 标准）
                ctx_json = ti.xcom_pull(task_ids=upstream_id)
                
                if ctx_json:
                    ctx = GovernanceContext.from_json(ctx_json)
                    self.log.info(f"✅ Restored context from upstream: {upstream_id}")
                    return ctx
                else:
                    self.log.warning(f"⚠️ Upstream task [{upstream_id}] returned None")
            except Exception as e:
                self.log.warning(f"⚠️ Failed to restore context from upstream: {e}")
        
        # 兜底：创建新 Context
        self.log.info("📦 Creating new context (no valid upstream found)")
        return self._create_new_context(context)
    
    def _create_new_context(self, context: Dict[str, Any]) -> GovernanceContext:
        """
        创建新 Context（兜底逻辑）
        
        生成规则：
        1. batch_id: 优先读 conf，否则用 "BATCH_{ts_nodash}"
        2. run_date: 优先读 conf，否则用 ds (YYYY-MM-DD)
        3. storage_type: 从全局配置读取
        
        Args:
            context: Airflow context 字典
        
        Returns:
            新的 GovernanceContext 对象
        """
        dag_run = context['dag_run']
        
        # 优先读取 conf（手动触发时会有）
        batch_id = None
        run_date = None
        
        if dag_run.conf:
            batch_id = dag_run.conf.get('batch_id')
            run_date = dag_run.conf.get('run_date')
        
        # 兜底：自动生成
        if not batch_id:
            batch_id = f"BATCH_{context['ts_nodash']}"
        if not run_date:
            run_date = context['ds']
        
        self.log.info(f"Generated context: batch_id={batch_id}, run_date={run_date}")
        
        return GovernanceContext(
            batch_id=batch_id,
            run_date=run_date,
            storage_type=Config.get_storage_type()
        )
    
    def _inject_io_strategy(self, ctx: GovernanceContext) -> None:
        """
        注入 IO 策略（验证配置一致性）
        
        说明：
        Context 序列化时不包含 IO 实例（不可序列化），
        但 Context 的方法内部会延迟导入 IOStrategy。
        这里主要是验证 storage_type 的一致性。
        
        Args:
            ctx: 治理上下文
        """
        global_storage_type = Config.get_storage_type()
        
        # 验证一致性
        if ctx.storage_type != global_storage_type:
            self.log.warning(
                f"⚠️ Context storage_type ({ctx.storage_type}) differs from "
                f"global config ({global_storage_type}). Using global config."
            )
            # 强制使用全局配置
            ctx.storage_type = global_storage_type
        
        self.log.info(f"IO Strategy: {ctx.storage_type}")
    
    # ========================================================================
    # 分区清理
    # ========================================================================
    
    def _clean_partition(self, ctx: GovernanceContext, stage: str, key: str) -> None:
        """
        清理分区目录（子类显式调用）
        
        使用场景：
        - Loader: 在写入每个 output_key 前调用
        - Adapter: 在写入 output_key 前调用
        - Rule: 在写入结果前调用
        
        Args:
            ctx: 治理上下文
            stage: 数据阶段 ("RAW" / "ENTITY" / "RESULT")
            key: 数据键名
        
        示例:
            self._clean_partition(ctx, stage="RAW", key="mysql_summary")
        """
        # 构建分区路径
        uri = ctx._build_partition_path(stage, key)
        
        # 检查是否存在
        if IOStrategy.exists(uri, ctx.storage_type):
            self.log.info(f"🧹 Cleaning partition: {uri}")
            IOStrategy.clean_directory(uri, ctx.storage_type)
        else:
            self.log.info(f"⏭️ Partition does not exist, skip cleaning: {uri}")
    
    def _clean_multiple_partitions(self, ctx: GovernanceContext, stage: str, keys: list) -> None:
        """
        批量清理多个分区（便捷方法）
        
        Args:
            ctx: 治理上下文
            stage: 数据阶段
            keys: 键名列表
        """
        for key in keys:
            self._clean_partition(ctx, stage, key)
    
    # ========================================================================
    # 配置管理
    # ========================================================================
    
    def _load_config(self) -> Dict[str, Any]:
        """
        加载配置（从字典或 YAML 文件）
        
        优先级：
        1. config_dict（直接传入）
        2. config_path（YAML 文件）
        3. 空字典（无配置）
        
        Returns:
            配置字典
        """
        # 优先使用 config_dict
        if self.config_dict:
            self.log.info("Loaded config from dict")
            return self.config_dict
        
        # 从文件加载
        if self.config_path:
            try:
                # 构建完整路径（相对于 $AIRFLOW_HOME/plugins/）
                airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
                full_path = os.path.join(airflow_home, 'plugins', self.config_path)
                
                # 读取 YAML
                with open(full_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                
                self.log.info(f"✅ Loaded config from: {full_path}")
                return config if config else {}
                
            except FileNotFoundError:
                self.log.error(f"❌ Config file not found: {full_path}")
                raise
            except yaml.YAMLError as e:
                self.log.error(f"❌ Invalid YAML syntax: {e}")
                raise
        
        # 无配置
        self.log.info("No config provided, using empty dict")
        return {}
    
    def _validate_config(self, required_keys: list) -> None:
        """
        验证配置完整性（可选，子类调用）
        
        Args:
            required_keys: 必需的配置键列表
        
        Raises:
            ValueError: 如果缺少必需的配置
        """
        missing = [key for key in required_keys if key not in self._config]
        
        if missing:
            raise ValueError(f"Missing required config keys: {', '.join(missing)}")
    
    # ========================================================================
    # 异常处理
    # ========================================================================
    
    def handle_error(self, error: Exception, context: Dict[str, Any]) -> None:
        """
        异常处理
        
        职责：
        1. 记录详细错误日志
        2. 预留：发送告警通知
        3. 预留：记录到数据库
        
        Args:
            error: 异常对象
            context: Airflow context 字典
        """
        self.log.error(f"❌ Task [{self.task_id}] failed: {error}")
        self.log.error(f"Error type: {type(error).__name__}")
        
        # 打印堆栈（调试用）
        import traceback
        self.log.error(f"Stacktrace:\n{traceback.format_exc()}")
        
        # 预留：发送告警
        # self._send_alert(error, context)
    
    # ========================================================================
    # 工具方法
    # ========================================================================
    
    def _get_airflow_context_summary(self, context: Dict[str, Any]) -> Dict[str, Any]:
        """
        获取 Airflow Context 摘要（调试用）
        
        Args:
            context: Airflow context 字典
        
        Returns:
            摘要字典
        """
        return {
            'dag_id': context['dag'].dag_id,
            'task_id': self.task_id,
            'execution_date': str(context['execution_date']),
            'run_id': context['dag_run'].run_id,
            'ds': context['ds'],
            'ts': context['ts'],
        }


# ============================================================================
# 便捷函数（供其他模块使用）
# ============================================================================

def get_upstream_context(ti, task_id: str) -> Optional[GovernanceContext]:
    """
    便捷函数：从上游 Task 获取 Context
    
    Args:
        ti: TaskInstance 对象
        task_id: 上游 Task ID
    
    Returns:
        GovernanceContext 对象（如果存在）
    """
    ctx_json = ti.xcom_pull(task_ids=task_id)
    if ctx_json:
        return GovernanceContext.from_json(ctx_json)
    return None


def get_multiple_upstream_contexts(ti, task_ids: list) -> list:
    """
    便捷函数：从多个上游 Task 获取 Context
    
    Args:
        ti: TaskInstance 对象
        task_ids: 上游 Task ID 列表
    
    Returns:
        Context 对象列表
    """
    contexts = []
    for task_id in task_ids:
        ctx = get_upstream_context(ti, task_id)
        if ctx:
            contexts.append(ctx)
    return contexts
