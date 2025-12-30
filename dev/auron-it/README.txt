

类命名与责任分配合适：
Main.scala：作为入口点（e.g., 可执行对象），适合 CI 脚本调用（如 scala Main --args），处理命令行参数并运行套件。
SuiteArgs.scala：专用处理测试参数（e.g., case class for dataPath, enablePlanCheck, updateGolden），便于配置化运行。
SessionManager.scala：管理 SparkSession（vanilla 和 auron 模式），包括创建、配置、加载表，避免重复代码。
QueryRunner.scala：核心执行逻辑，负责运行查询、捕获结果/耗时/计划，抽象了执行流程。
Suite.scala：测试套件基类，提供通用 setup/teardown 和报告逻辑。
comparison 子包：QueryResultComparator.scala 处理结果一致性（e.g., DataFrame 比较），PlanStabilityChecker.scala 处理计划稳定性（捕获、规范化、比较 golden files），分离了比较concern。
tpcds 子包：AuronTPCDSSuite.scala 实现 Auron 特定的 TPC-DS 测试套件（继承 Suite），TPCDSFeatures.scala 可能定义 TPC-DS 特性（如查询加载、数据生成），保持了领域特定性。