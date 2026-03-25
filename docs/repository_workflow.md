# Repository Workflow

## 目标

这份文档定义这个独立仓库后续的日常协作和发布方式，重点是：

- 不把研究过程搞乱
- 不让主线回测被临时试验污染
- 让每次重要变化都能追溯

## 分支约定

默认保留三类分支：

- `main`
  - 永远代表当前稳定主线
  - 只放已经验证过、可以继续作为日常基线运行的版本

- `codex/<topic>`
  - 用于具体改动或专题实验
  - 例如：
    - `codex/optimizer-round-2`
    - `codex/data-fallback`
    - `codex/review-automation`

- `research/<topic>`
  - 用于更偏研究型、可能短期不合并的实验
  - 例如：
    - `research/factor-rotation`
    - `research/industry-overlay`

## 日常开发流程

推荐流程：

1. 从 `main` 拉出新分支
2. 在分支里做改动
3. 先跑最小验证
4. 再合回 `main`

示例：

```bash
git checkout main
git pull
git checkout -b codex/slowrebalance-tune
```

改完后：

```bash
git status
git add .
git commit -m "Tune slow rebalance parameters"
git push -u origin codex/slowrebalance-tune
```

确认稳定后再合并回 `main`。

## 提交信息建议

尽量用清楚的英文短句，避免“update”这种低信息量描述。

推荐风格：

- `Add standalone eastmoney client for v2`
- `Refine README and repository workflow docs`
- `Tune portfolio rebalance constraints`
- `Add paper account daily tracking`

## 什么时候可以直接进 main

满足下面条件时，可以直接合并到 `main`：

- 改动范围明确
- 最小验证已通过
- 不会破坏当前主线工作流
- 文档与代码口径一致

如果属于下面情况，建议先走实验分支：

- 改动会改变主线参数
- 会影响历史回测解释
- 会重构数据层或执行层
- 只是探索性研究，还不确定是否保留

## 发布节奏建议

不需要过度正式，但建议保留“阶段性发布”的习惯。

推荐两种发布点：

- `研究里程碑`
  - 例如：完成一轮主线参数优化
- `系统里程碑`
  - 例如：完成纸面账户跟踪闭环

## 标签建议

推荐用轻量标签管理关键节点：

- `v0.1.0`
  - 独立仓库初始化
- `v0.2.0`
  - 主线参数稳定
- `v0.3.0`
  - 每日复盘与模拟账户闭环

打标签示例：

```bash
git tag -a v0.1.0 -m "Initial standalone multifactor strategy project"
git push origin v0.1.0
```

## 报告文件管理建议

`reports/` 不建议无节制堆积。

推荐做法：

- 保留少量代表性样例在仓库里
- 大量实验结果按目录归档
- 临时跑数目录加入 `.gitignore`

适合保留进仓库的内容：

- 当前主线基线回测
- 当前纸面账户快照
- 有代表性的每日复盘样例

不建议长期保留进主线仓库的内容：

- 大量重复实验输出
- 中间缓存
- 临时 smoke 报告

## 合并前最小检查

每次准备合并到 `main` 前，至少做：

```bash
PYTHONPATH=src python3 -m compileall src/qstrategy_v2
PYTHONPATH=src python3 -m qstrategy_v2.cli --help
```

如有对应测试，再补：

```bash
PYTHONPATH=src python3 - <<'PY'
# run targeted tests here
PY
```

## 推荐的维护原则

- 主线少改大逻辑，多做可验证的小步迭代
- 研究型实验和稳定主线分开
- 文档和代码一起更新
- 每次重要改动都留下原因，而不是只留下结果

一句话总结：

`把 main 当成可持续运行的主线，把分支当成安全试验场。`
