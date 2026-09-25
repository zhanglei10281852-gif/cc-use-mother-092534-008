# 后台任务能力票据调度

后台任务定义、能力票据、租约执行、补偿与恢复的领域资料。仓库提供领域合同、策略样例和事件资料，供后续建立完整服务时统一对象、状态、时间与审计语义。

## 目录

- `domain/contract.json`：实体、状态、事件类型和关键业务规则。
- `domain/policies.json`：可被程序读取的策略样例。
- `examples/events.json`：按业务发生时间排列的事件样例。
- `tools/validate_contract.py`：使用 Python 标准库和 SQLite 内存表验证资料一致性。

## 构建

```bash
python3 -m compileall -q .
```

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 资料校验

```bash
python3 tools/validate_contract.py
```

所有命令都在项目根目录执行，不需要另行启动数据库、缓存或其他服务。
