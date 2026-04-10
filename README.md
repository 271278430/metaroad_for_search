# 学科学段知识图谱

基于 Neo4j 图数据库的学科知识图谱展示系统。通过元路径（Metapath）定义的知识结构，自动从图数据库中提取各学科的知识层级关系，并以 Web 页面形式展示。

## 项目结构

```
meta_road/
├── web_server.py              # Flask Web 展示服务（主入口）
├── meta_road.py               # 元路径挖掘核心模块
├── aggregate_metapaths.py     # 元路径聚合与 Cypher 生成
├── generate_md.py             # Markdown 批量生成脚本
├── validate_metapaths.py      # 元路径验证工具
├── save_metaroad.py           # 元路径保存工具
├── requirements.txt           # Python 依赖
├── metapaths/                 # 各学科元路径配置（JSON）
│   ├── 义教化学.json
│   ├── 义教历史.json
│   ├── 高中数学.json
│   └── ...
├── md_output/                 # 自动生成的缓存与 Markdown（运行后生成）
└── static/
    └── index.html             # 前端页面
```

## 核心设计

### 数据查询：2 查询 + Trie DFS

不逐条元路径查询 Neo4j，而是：

1. **查询 1**：一次性拉取某学科全部节点
2. **查询 2**：一次性拉取某学科全部关系
3. **Trie DFS**：将元路径构建为类型前缀树（Trie），在内存中用 Trie 约束 DFS 遍历图，一步构建展示树

### 三级缓存

- 内存缓存 → 文件缓存 `md_output/{学科}/_cache.json` → Neo4j 实时查询
- 首次查询后自动缓存，支持一键刷新

### 前端展示

每个章节页面包含：
- **结构概览**：紧凑的树形结构图，展示节点间的层级关系
- **文本内容**：扁平化展示每个节点的类型、标题和属性详情

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置 Neo4j 连接

编辑 `web_server.py` 顶部的配置：

```python
NEO4J_URI = 'bolt://<host>:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = '<password>'
```

### 3. 启动服务

```bash
python web_server.py
```

访问 http://localhost:5000

## 支持的学科

义教：化学、历史、地理、数学、物理、生物、英语、语文、道法

高中：化学、历史、地理、政治、数学、物理、生物、英语、语文

## API 接口

| 接口 | 说明 |
|------|------|
| `GET /api/subjects` | 返回全部学科列表 |
| `GET /api/<学科>/chapters` | 返回某学科的章节列表 |
| `GET /api/<学科>/chapter/<id>` | 返回某章节的完整知识树 |
| `POST /api/cache/clear` | 清除缓存，强制重新查询 |

## 元路径格式

存储在 `metapaths/<学科>.json`，每条元路径定义一种知识关联路径：

```json
[
  "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)",
  "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)"
]
```

起始类型不同的学科（如义教化学以 `Unit` 为起始）也能正常工作。
