"""
web_server.py - 学科知识图谱 Web 展示服务

2 次查询拿全量节点和边，用 Trie 约束 DFS 一次性构建展示树。
首次查询后自动缓存到 md_output/{学科}/ 目录（JSON + Markdown）。

启动: python web_server.py
访问: http://localhost:5000
"""

from flask import Flask, jsonify, send_from_directory
from neo4j import GraphDatabase
from collections import defaultdict
import json, re, os, shutil

app = Flask(__name__, static_folder='static')

# ==================== 配置 ====================
NEO4J_URI = 'bolt://10.50.243.143:7687'
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'neo4j123'
METAPATH_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'metapaths')
MD_OUTPUT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'md_output')

SUBJECTS = [
    '义教化学', '义教历史', '义教地理', '义教数学', '义教物理',
    '义教生物', '义教英语', '义教语文', '义教道法',
    '高中化学', '高中历史', '高中地理', '高中政治', '高中数学',
    '高中物理', '高中生物', '高中英语', '高中语文',
]

driver = GraphDatabase.driver(
    NEO4J_URI,
    auth=(NEO4J_USER, NEO4J_PASSWORD),
    connection_timeout=30,
    max_transaction_retry_time=60,
)
_cache = {}


# ==================== 工具函数 ====================

def extract_types(pattern):
    """从元路径模式提取节点类型列表，如 '(:A)-[]-(:B)' → ['A', 'B']"""
    return re.findall(r'\(:([^)]+)\)', pattern)


def load_metapaths(subject):
    """加载学科的元路径列表"""
    fp = os.path.join(METAPATH_DIR, f'{subject}.json')
    if not os.path.exists(fp):
        return []
    with open(fp, 'r', encoding='utf-8') as f:
        return json.load(f)


def clean_props(props):
    """保留全部属性，仅去 CJ_ 前缀，空值跳过"""
    clean = {}
    for k, v in props.items():
        dk = k[3:] if k.startswith('CJ_') else k
        if isinstance(v, (list, dict)):
            clean[dk] = v
        elif v is not None and str(v).strip():
            clean[dk] = str(v)
    return clean


def build_trie(metapaths):
    """将元路径列表转为类型前缀树（Trie）。
    跳过起始类型，从第二个类型开始建树。
    '__end__' 标记表示该位置是某条元路径的合法终点。
    """
    root = {}
    for pattern in metapaths:
        types = extract_types(pattern)
        node = root
        for t in types[1:]:
            if t not in node:
                node[t] = {}
            node = node[t]
        node['__end__'] = True
    return root


def safe_filename(name):
    """将标识符转为安全文件名"""
    return re.sub(r'[\\/:*?"<>|]', '_', name)


# ==================== 核心查询逻辑 ====================

def _query_neo4j(subject):
    """从 Neo4j 查询某学科的全部数据（2 次查询 + Trie DFS 一步建树）"""
    mps = load_metapaths(subject)
    if not mps:
        return None

    start_type = extract_types(mps[0])[0]
    trie = build_trie(mps)

    with driver.session() as session:
        # 查询 1：该学科全部节点
        nodes = {}
        nr = session.run(
            "MATCH (n) WHERE n.subject = $s "
            "RETURN n.identifier AS id, properties(n) AS p",
            s=subject,
        )
        for r in nr:
            p = dict(r['p'])
            nid = r['id']
            nodes[nid] = {
                'id': nid,
                'type': p.get('type', ''),
                'title': p.get('title', '未知'),
                'props': clean_props(p),
            }

        # 查询 2：该学科全部关系（无向，去重）
        edges = defaultdict(set)
        nr2 = session.run(
            "MATCH (a)-[r]-(b) "
            "WHERE a.subject = $s AND b.subject = $s AND a.identifier < b.identifier "
            "RETURN a.identifier AS from_id, b.identifier AS to_id",
            s=subject,
        )
        for r in nr2:
            edges[r['from_id']].add(r['to_id'])
            edges[r['to_id']].add(r['from_id'])

    # 提取起始节点
    start_nodes = {nid: nodes[nid] for nid, n in nodes.items() if n['type'] == start_type}

    # 一步到位：Trie DFS 直接构建每个章节的展示树
    chapter_trees = {}
    for start_id in start_nodes:
        visited = set()
        tree = _build_chapter_tree(start_id, trie, nodes, edges, visited)
        if tree:
            chapter_trees[start_id] = tree

    return {
        'start_nodes': start_nodes,
        'chapter_trees': chapter_trees,
        'start_type': start_type,
    }


def _build_chapter_tree(node_id, trie_node, nodes, edges, visited):
    """一步到位：Trie 约束 DFS，直接构建展示树，同时处理去重"""
    node = nodes.get(node_id)
    if not node:
        return None

    if node_id in visited:
        return {
            'node': {'id': node['id'], 'type': node['type'], 'title': node['title']},
            'children': [],
            'ref': True,
        }

    visited.add(node_id)

    children = []
    for neighbor_id in sorted(edges.get(node_id, set())):
        neighbor = nodes.get(neighbor_id)
        if not neighbor:
            continue
        ntype = neighbor['type']
        child_trie = trie_node.get(ntype)
        if child_trie is None:
            continue
        child = _build_chapter_tree(neighbor_id, child_trie, nodes, edges, visited)
        if child:
            children.append(child)

    return {'node': node, 'children': children}


def get_subject_data(subject):
    """
    获取某学科的图数据。优先级：
    1. 内存缓存 → 直接返回
    2. 文件缓存 md_output/{学科}/_cache.json → 加载到内存
    3. 查询 Neo4j → 存文件缓存 + 生成 Markdown
    """
    if subject in _cache:
        return _cache[subject]

    # 检查文件缓存
    cache_dir = os.path.join(MD_OUTPUT_DIR, subject)
    cache_file = os.path.join(cache_dir, '_cache.json')
    if os.path.exists(cache_file):
        try:
            with open(cache_file, 'r', encoding='utf-8') as f:
                data = json.load(f)
            _cache[subject] = data
            print(f'[INFO] 从文件缓存加载: {subject}')
            return data
        except Exception as e:
            print(f'[WARN] 缓存文件损坏，将重新查询: {e}')

    # 查询 Neo4j
    print(f'[INFO] 查询 Neo4j: {subject} ...')
    data = _query_neo4j(subject)
    if not data:
        return None

    # 写入文件缓存
    os.makedirs(cache_dir, exist_ok=True)
    try:
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
        print(f'[INFO] 已缓存到: {cache_file}')
    except Exception as e:
        print(f'[WARN] 缓存文件保存失败: {e}')

    # 生成所有章节的 Markdown 文件
    _generate_all_md(subject, data)

    _cache[subject] = data
    return data


# ==================== Markdown 生成 ====================

def tree_to_md(tree, level=1):
    """将树结构递归转为 Markdown 文本"""
    if not tree:
        return ''
    node = tree['node']
    prefix = '#' * min(level, 6)

    # 引用节点 — 只输出一行标记
    if tree.get('ref'):
        return f'{prefix} [{node["type"]}] {node["title"]} *(见上文)*\n\n'

    md = f'{prefix} [{node["type"]}] {node["title"]}\n\n'

    props = node.get('props', {})
    for k, v in props.items():
        if isinstance(v, list):
            md += f'- **{k}**: ' + ', '.join(str(x) for x in v) + '\n'
        elif isinstance(v, dict):
            md += f'- **{k}**: {json.dumps(v, ensure_ascii=False)}\n'
        else:
            md += f'- **{k}**: {v}\n'
    if props:
        md += '\n'

    for child in tree.get('children', []):
        md += tree_to_md(child, level + 1)
    return md


def _generate_all_md(subject, data):
    """为某学科的所有章节生成 Markdown 文件"""
    md_dir = os.path.join(MD_OUTPUT_DIR, subject)
    os.makedirs(md_dir, exist_ok=True)
    count = 0
    for start_id, tree in data.get('chapter_trees', {}).items():
        title = data['start_nodes'].get(start_id, {}).get('title', start_id)
        fname = safe_filename(f'{title}.md')
        md_path = os.path.join(md_dir, fname)
        with open(md_path, 'w', encoding='utf-8') as f:
            f.write(tree_to_md(tree))
        count += 1
    print(f'[INFO] 已生成 {count} 个 Markdown 文件: {md_dir}/')


# ==================== API 路由 ====================

@app.route('/')
def index():
    return send_from_directory('static', 'index.html')


@app.route('/api/subjects')
def api_subjects():
    """返回全部学科列表"""
    res = []
    for s in SUBJECTS:
        mps = load_metapaths(s)
        res.append({'name': s, 'metapath_count': len(mps), 'has_data': len(mps) > 0})
    return jsonify(res)


@app.route('/api/<subject>/chapters')
def api_chapters(subject):
    """返回某学科的章节（Chapter / Unit）列表"""
    d = get_subject_data(subject)
    if not d:
        return jsonify({'error': '该学科无元路径数据'}), 404
    chs = [{'id': nid, 'title': n['title'], 'type': n['type']}
           for nid, n in d['start_nodes'].items()]
    chs.sort(key=lambda x: x['id'])
    return jsonify(chs)


@app.route('/api/<subject>/chapter/<path:cid>')
def api_chapter(subject, cid):
    """返回某章节的完整元路径树"""
    d = get_subject_data(subject)
    if not d:
        return jsonify({'error': '无数据'}), 404
    tree = d.get('chapter_trees', {}).get(cid)
    if not tree:
        return jsonify({'error': '章节未找到'}), 404
    return jsonify(tree)


@app.route('/api/cache/clear', methods=['POST'])
def clear_cache():
    """清除所有缓存（内存 + 文件），强制重新查询 Neo4j"""
    _cache.clear()
    if os.path.exists(MD_OUTPUT_DIR):
        shutil.rmtree(MD_OUTPUT_DIR)
    print('[INFO] 已清除全部缓存')
    return jsonify({'status': 'ok'})


if __name__ == '__main__':
    print('启动服务器: http://127.0.0.1:5000')
    app.run(host='127.0.0.1', port=5000, debug=True)
