"""
orchestration/rule_scanner.py
规则扫描器

职责：
- 扫描规则配置文件（configs/rules/*.yaml）
- 解析规则依赖关系
- 拓扑排序（处理依赖顺序）
- 验证配置完整性（防御性解析）
- 支持 V2 规则格式（filter / aggregate 类型）

运行时机：
- DAG 文件顶层代码（解析时）
- Aggregator 执行时（加载规则元数据）
- 不能有耗时 I/O 操作（只读本地文件）
"""
import os
import glob
import yaml
from typing import List, Dict, Any, Optional


class RuleScanner:
    """
    规则扫描器
    
    使用示例:
    ```python
    # 在 DAG 文件顶层代码中
    scanner = RuleScanner()
    rules = scanner.scan_rules()
    
    # 动态生成 Rule Tasks
    for rule in rules:
        GenericRuleOperator(
            task_id=rule['rule_id'],
            config_path=rule['config_path'],
            ...
        )
    ```
    """
    
    def __init__(self, rules_dir: str = None):
        """
        初始化
        
        Args:
            rules_dir: 规则目录路径（相对于 $AIRFLOW_HOME/plugins/）
        """
        self.rules_dir = rules_dir or 'configs/rules'
    
    def scan_rules(self) -> List[Dict[str, Any]]:
        """
        扫描规则并返回排序后的列表
        
        Returns:
            规则配置列表（已按依赖关系拓扑排序）
            
        示例:
            [
                {
                    'rule_id': 'rule_p0_time_check',
                    'config_path': 'configs/rules/p0_time_check.yaml',
                    'depends_on': [],
                    'severity': 'P0',
                    'config': {...}
                },
                ...
            ]
        
        Raises:
            ValueError: 配置验证失败、循环依赖等
        """
        # 1. 扫描目录
        rule_files = self._scan_rule_files()
        
        if not rule_files:
            raise ValueError(f"No rule files found in {self.rules_dir}")
        
        print(f"📋 Found {len(rule_files)} rule files")
        
        # 2. 加载并验证规则
        rules = []
        for file_path in rule_files:
            try:
                rule_config = self._load_and_validate_rule(file_path)
                if rule_config is None:
                    # 空文件，跳过
                    print(f"⏭️ Skipping empty rule file: {file_path}")
                    continue
                rules.append(rule_config)
            except Exception as e:
                # 配置错误：拒绝加载 DAG
                raise ValueError(f"Failed to load rule from {file_path}: {e}")
        
        # 3. 拓扑排序（处理依赖关系）
        sorted_rules = self._topological_sort(rules)
        
        print(f"✅ Loaded {len(sorted_rules)} rules (topologically sorted)")
        
        return sorted_rules
    
    def get_rule_configs_by_id(self, target_entity: Optional[str] = None) -> Dict[str, Dict]:
        """
        获取规则配置字典（按 rule_id 索引）
        
        用于 Aggregator 在执行时获取规则的完整配置，
        以驱动数据驱动的 alert/asset 处理逻辑。
        
        Args:
            target_entity: 可选，按 target_entity 过滤
        
        Returns:
            {rule_id: full_config_dict}
        """
        rules = self.scan_rules()
        
        result = {}
        for rule in rules:
            if target_entity and rule.get('target_entity') != target_entity:
                continue
            result[rule['rule_id']] = rule['config']
        
        return result
    
    def _scan_rule_files(self) -> List[str]:
        """
        扫描规则目录，返回所有 YAML 文件路径
        
        支持绝对路径和相对路径（相对于 $AIRFLOW_HOME/plugins/）
        
        Returns:
            文件路径列表（已排序）
        """
        if os.path.isabs(self.rules_dir):
            rules_path = self.rules_dir
        else:
            airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
            rules_path = os.path.join(airflow_home, 'plugins', self.rules_dir)
        
        # 扫描 *.yaml 文件
        pattern = os.path.join(rules_path, '*.yaml')
        files = glob.glob(pattern)
        
        # 排序保证稳定性
        return sorted(files)
    
    def _load_and_validate_rule(self, file_path: str) -> Optional[Dict[str, Any]]:
        """
        加载并验证单个规则配置
        
        Args:
            file_path: 规则文件路径
        
        Returns:
            规则配置字典
        
        Raises:
            ValueError: 配置验证失败
        """
        # 1. 加载 YAML
        with open(file_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        # 空文件或空内容：跳过
        if config is None:
            return None
        
        # 2. 防御性验证（Schema Validation）
        self._validate_rule_config(config, file_path)
        
        # 3. 提取关键信息
        # 计算相对路径（相对于 plugins/）
        airflow_home = os.getenv('AIRFLOW_HOME', '/opt/airflow')
        plugins_dir = os.path.join(airflow_home, 'plugins')
        relative_path = os.path.relpath(file_path, plugins_dir)
        
        return {
            'rule_id': config['meta']['rule_id'],
            'config_path': relative_path,  # 相对路径
            'depends_on': config.get('depends_on', []),
            'severity': config['meta']['severity'],
            'description': config['meta'].get('description', ''),
            'target_entity': config.get('target_entity'),
            'config': config  # 完整配置（供调试和 Aggregator 使用）
        }
    
    def _validate_rule_config(self, config: Dict, file_path: str) -> None:
        """
        验证规则配置完整性（防御性解析，支持 V2 格式）
        
        V2 格式新增支持：
        - logic.type: filter（默认） | aggregate
        - logic.pre_compute: 预计算步骤（可选）
        - logic.group_by / agg_exprs / having_expr: 聚合型规则必需
        - input.entity_key: 明确的实体读取键（可选）
        - alert.content_template: Jinja 模板（可选）
        - alert.trigger_id_field: 触发 ID 字段名（可选）
        - asset.field_mapping: 资产字段映射（可选）
        
        必需字段：
        - meta
        - meta.rule_id
        - meta.severity
        - target_entity
        - logic
        - logic.filter_expr（当 type=filter 时）
        - logic.group_by / agg_exprs / having_expr（当 type=aggregate 时）
        
        Args:
            config: 规则配置字典
            file_path: 文件路径（用于错误提示）
        
        Raises:
            ValueError: 配置验证失败
        """
        errors = []
        
        # ========== meta 校验 ==========
        if 'meta' not in config:
            errors.append("Missing 'meta' field")
        else:
            if 'rule_id' not in config['meta']:
                errors.append("Missing 'meta.rule_id'")
            if 'severity' not in config['meta']:
                errors.append("Missing 'meta.severity'")
            else:
                # 验证 severity 取值
                valid_severities = ['P0', 'P1', 'P2', 'INFO']
                if config['meta']['severity'] not in valid_severities:
                    errors.append(f"Invalid severity: {config['meta']['severity']} (must be one of {valid_severities})")
        
        # ========== target_entity 校验 ==========
        if 'target_entity' not in config:
            errors.append("Missing 'target_entity'")
        
        # ========== logic 校验（支持 V2 多类型） ==========
        if 'logic' not in config:
            errors.append("Missing 'logic' field")
        else:
            logic = config['logic']
            logic_type = logic.get('type', 'filter')  # 默认 filter（向后兼容）
            
            if logic_type == 'filter':
                # filter 类型必须有 filter_expr
                if 'filter_expr' not in logic:
                    errors.append("Missing 'logic.filter_expr' (required for type=filter)")
            
            elif logic_type == 'aggregate':
                # aggregate 类型必须有 group_by, agg_exprs, having_expr
                if 'group_by' not in logic:
                    errors.append("Missing 'logic.group_by' (required for type=aggregate)")
                elif not isinstance(logic['group_by'], list):
                    errors.append("'logic.group_by' must be a list of column names")
                
                if 'agg_exprs' not in logic:
                    errors.append("Missing 'logic.agg_exprs' (required for type=aggregate)")
                elif not isinstance(logic['agg_exprs'], list):
                    errors.append("'logic.agg_exprs' must be a list")
                else:
                    for idx, agg in enumerate(logic['agg_exprs']):
                        if 'name' not in agg:
                            errors.append(f"Missing 'name' in logic.agg_exprs[{idx}]")
                        if 'expr' not in agg:
                            errors.append(f"Missing 'expr' in logic.agg_exprs[{idx}]")
                
                if 'having_expr' not in logic:
                    errors.append("Missing 'logic.having_expr' (required for type=aggregate)")
            
            elif logic_type == 'cross_entity':
                # cross_entity 类型：跨表校验（anti_join / compare）
                # 必须有 input.entity_keys、logic.operation、logic.join_on
                
                # 验证 input.entity_keys
                entity_keys = config.get('input', {}).get('entity_keys')
                if not entity_keys:
                    errors.append("Missing 'input.entity_keys' (required for type=cross_entity)")
                else:
                    if 'left' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.left'")
                    if 'right' not in entity_keys:
                        errors.append("Missing 'input.entity_keys.right'")
                
                # 验证 operation
                operation = logic.get('operation')
                if not operation:
                    errors.append("Missing 'logic.operation' (required for type=cross_entity)")
                elif operation not in ('anti_join', 'compare'):
                    errors.append(f"Invalid operation: '{operation}' (must be 'anti_join' or 'compare')")
                
                # 验证 join_on
                join_on = logic.get('join_on')
                if not join_on:
                    errors.append("Missing 'logic.join_on' (required for type=cross_entity)")
                else:
                    if 'left' not in join_on:
                        errors.append("Missing 'logic.join_on.left'")
                    elif not isinstance(join_on['left'], list):
                        errors.append("'logic.join_on.left' must be a list of column names")
                    if 'right' not in join_on:
                        errors.append("Missing 'logic.join_on.right'")
                    elif not isinstance(join_on['right'], list):
                        errors.append("'logic.join_on.right' must be a list of column names")
                
                # compare 需要 assertion
                if operation == 'compare' and 'assertion' not in logic:
                    errors.append("Missing 'logic.assertion' (required for operation=compare)")
                
                # 验证 left_pre_agg / right_pre_agg（可选）
                for side in ('left_pre_agg', 'right_pre_agg'):
                    if side in logic:
                        pre_agg = logic[side]
                        if 'group_by' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.group_by'")
                        if 'agg_exprs' not in pre_agg:
                            errors.append(f"Missing 'logic.{side}.agg_exprs'")
                        elif isinstance(pre_agg['agg_exprs'], list):
                            for idx, agg in enumerate(pre_agg['agg_exprs']):
                                if 'name' not in agg:
                                    errors.append(f"Missing 'name' in logic.{side}.agg_exprs[{idx}]")
                                if 'expr' not in agg:
                                    errors.append(f"Missing 'expr' in logic.{side}.agg_exprs[{idx}]")
            
            else:
                errors.append(f"Unknown logic type: '{logic_type}' (must be 'filter', 'aggregate', or 'cross_entity')")
            
            # pre_compute 校验（可选，两种类型都支持）
            if 'pre_compute' in logic:
                if not isinstance(logic['pre_compute'], list):
                    errors.append("'logic.pre_compute' must be a list")
                else:
                    for idx, step in enumerate(logic['pre_compute']):
                        if 'name' not in step:
                            errors.append(f"Missing 'name' in logic.pre_compute[{idx}]")
                        if 'expr' not in step:
                            errors.append(f"Missing 'expr' in logic.pre_compute[{idx}]")
        
        # ========== asset 校验（可选） ==========
        if 'asset' in config:
            asset = config['asset']
            if asset.get('enabled', False) and 'field_mapping' in asset:
                field_map = asset['field_mapping']
                required_mapping_fields = ['asset_id', 'vehicle_id', 'start_ts', 'end_ts']
                for f in required_mapping_fields:
                    if f not in field_map:
                        errors.append(f"Missing 'asset.field_mapping.{f}' (required when asset.enabled=true)")
        
        # ========== 汇总错误 ==========
        if errors:
            error_msg = f"Invalid rule config in {file_path}:\n" + "\n".join(f"  - {e}" for e in errors)
            raise ValueError(error_msg)
    
    def _topological_sort(self, rules: List[Dict]) -> List[Dict]:
        """
        拓扑排序（Kahn 算法）
        
        处理规则依赖关系，确保：
        - 被依赖的规则先执行
        - 检测循环依赖
        
        Args:
            rules: 规则列表
        
        Returns:
            排序后的规则列表
        
        Raises:
            ValueError: 检测到循环依赖
        """
        # 构建依赖图和规则字典
        graph = {}
        rule_dict = {}
        
        for rule in rules:
            rule_id = rule['rule_id']
            graph[rule_id] = rule['depends_on']
            rule_dict[rule_id] = rule
        
        # 验证依赖是否存在
        for rule_id, deps in graph.items():
            for dep in deps:
                if dep not in graph:
                    raise ValueError(
                        f"Rule '{rule_id}' depends on '{dep}', but '{dep}' not found. "
                        f"Available rules: {list(graph.keys())}"
                    )
        
        # 计算入度（我依赖多少个规则）
        # depends_on 的含义：我依赖谁（即存在边 dep -> me）
        # 入度 = len(depends_on)
        in_degree = {rule_id: len(deps) for rule_id, deps in graph.items()}
        
        # Kahn 算法：从入度为 0 的节点开始
        queue = [rule_id for rule_id, degree in in_degree.items() if degree == 0]
        result = []
        
        while queue:
            # 取出一个入度为 0 的节点（没有依赖的规则，可以先执行）
            rule_id = queue.pop(0)
            result.append(rule_dict[rule_id])
            
            # 遍历所有规则，找出"依赖于当前规则"的规则
            for other_rule_id, deps in graph.items():
                if rule_id in deps:
                    # 当前规则已完成，依赖它的规则入度减 1
                    in_degree[other_rule_id] -= 1
                    # 如果入度变为 0，说明所有依赖都已满足，可以执行了
                    if in_degree[other_rule_id] == 0:
                        queue.append(other_rule_id)
        
        # 检测循环依赖
        if len(result) != len(rules):
            # 找出参与循环依赖的规则
            unprocessed = [r['rule_id'] for r in rules if r not in result]
            raise ValueError(
                f"Circular dependency detected in rules! "
                f"Unprocessed rules: {unprocessed}. "
                f"Check the 'depends_on' fields."
            )
        
        return result
    
    def get_rule_summary(self, rules: List[Dict]) -> str:
        """
        生成规则摘要（用于日志）
        
        Args:
            rules: 规则列表
        
        Returns:
            摘要字符串
        """
        lines = ["Rule Execution Order:"]
        for idx, rule in enumerate(rules, 1):
            deps_str = f" (depends on: {', '.join(rule['depends_on'])})" if rule['depends_on'] else ""
            logic_type = rule['config'].get('logic', {}).get('type', 'filter')
            lines.append(f"  {idx}. [{rule['severity']}] {rule['rule_id']} (type={logic_type}){deps_str}")
        
        return "\n".join(lines)
