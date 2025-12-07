import logging
import traceback
from rules.external_cross.cnt_cycles import CntCyclesLogicRule 
from rules.single.cnt_newcycles import CntNewCyclesLogicRule 

logger = logging.getLogger("airflow.task")

def run_logic_check(cactus_conn_id, ngen_conn_id, ngen_table_name, date_filter, target_table_type="cnt_cycles"):
    """
    通用逻辑校验执行引擎 (Runner)
    
    :param cactus_conn_id: 自采数据库连接ID
    :param ngen_conn_id: nGen数据库连接ID
    :param ngen_table_name: nGen表名
    :param date_filter: 日期过滤条件
    :param target_table_type: 目标表类型 ('cnt_cycles' 或 'cnt_newcycles')
    """
    logger.info(f"🚀 开始逻辑校验 | 类型: {target_table_type} | 日期: {date_filter}")
    
    # ========================================================
    # 1. 策略选择 (Strategy Selection)
    # ========================================================
    if target_table_type == "cnt_newcycles":
        RuleClass = CntNewCyclesLogicRule
        table_label = "KPI: cnt_newcycles (单表时序检查)"
    else:
        # 默认为旧表逻辑
        RuleClass = CntCyclesLogicRule
        table_label = f"KPI: cnt_cycles vs nGen: {ngen_table_name}"

    # ========================================================
    # 2. 数据加载与清洗 (Load & ETL)
    # ========================================================
    try:
        df_self, df_other = RuleClass.load_data(
            cactus_conn_id, ngen_conn_id, ngen_table_name, date_filter
        )
    except Exception as e:
        err_msg = f"数据加载阶段发生严重错误: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        return {"status": "ERROR", "violation_count": 0, "report_text": err_msg}
    
    # 2.1 检查主表数据 (必须有)
    if df_self is None:
        msg = f"未查询到主表数据 ({table_label})，跳过校验。"
        logger.info(msg)
        return {"status": "SKIPPED", "violation_count": 0, "report_text": msg}
    
    # 2.2 检查 nGen 数据 (按需检查)
    # 【修复点】如果是 cnt_newcycles，允许 df_other 为空，不报错跳过
    if target_table_type != "cnt_newcycles":
        if df_other is None:
            msg = f"未查询到 nGen 数据 (虽然 KPI 有数据)，跳过校验。"
            logger.info(msg)
            return {"status": "SKIPPED", "violation_count": 0, "report_text": msg}
    
    # ========================================================
    # 3. 执行校验规则 (Run Checks)
    # ========================================================
    logger.info(f"数据加载完成，开始执行规则校验... (主表行数: {df_self.height})")
    
    try:
        check_results = RuleClass.run_checks(df_self, df_other)
    except Exception as e:
        err_msg = f"规则执行阶段发生错误: {str(e)}\n{traceback.format_exc()}"
        logger.error(err_msg)
        return {"status": "ERROR", "violation_count": 0, "report_text": err_msg}
    
    # ========================================================
    # 4. 生成汇总报告 (Report Generation)
    # ========================================================
    final_status = "SUCCESS"
    total_violation = 0
    
    log_buffer = []
    log_buffer.append(f"=== 🛡️ 数据质量逻辑校验报告 ===")
    log_buffer.append(f"对象: {table_label}")
    log_buffer.append(f"日期: {date_filter}")
    log_buffer.append("-" * 40)
    
    if not check_results:
        log_buffer.append("⚠️ 未定义任何校验规则。")
    
    for res in check_results:
        check_type = res.get('type', 'Unknown Check')
        matched = res.get('total_matched', 0)
        passed = res.get('passed', 0)
        failed = res.get('failed', 0)
        
        log_buffer.append(f"🔍 检查项: {check_type}")
        log_buffer.append(f"   • 覆盖数据量: {matched}")
        log_buffer.append(f"   • ✅ 通过: {passed}")
        
        if failed > 0:
            final_status = "FAILED"
            total_violation += failed
            log_buffer.append(f"   • ❌ 异常: {failed}")
            
            # 如果有异常样本，打印出来
            samples = res.get('failed_samples')
            if samples is not None and not samples.is_empty():
                log_buffer.append(f"\n   [异常样本 Top 10]:\n{samples}")
        else:
            log_buffer.append(f"   • 结果: 完美")
        
        log_buffer.append("-" * 40)
            
    report_text = "\n".join(log_buffer)
    
    # 打印到 Airflow 日志
    logger.info(report_text)
    
    # 返回结构化结果
    return {
        "status": final_status,
        "violation_count": total_violation,
        "report_text": report_text # 这个字段将被邮件发送
    }