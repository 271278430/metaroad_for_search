import json, os, re
from neo4j import GraphDatabase
from collections import defaultdict

SUBJECTS = [
    '义教化学', '义教历史', '义教地理', '义教数学', '义教物理', '义教生物', '义教英语', '义教语文', '义教道法',
    '高中化学', '高中历史', '高中地理', '高中政治', '高中数学', '高中物理', '高中生物', '高中英语', '高中语文',
]

METAPATH_DIR = '/data/shanghui/meta_road/metapaths'
OUTPUT_DIR = '/data/shanghui/meta_road/md_output'

NEO4J_URI = "bolt://10.50.243.143:7687"
NEO4J_USER = 'neo4j'
NEO4J_PASSWORD = 'neo4j123'

# 教科书结构层级类型（章节体系）——只有这些类型会递归遍历
STRUCTURAL_TYPES = {'Chapter', 'Section', 'SubSection', 'KeyPoint', 'Unit'}

# 输出时跳过的系统字段
SKIP_FIELDS = {'contentJson', 'identifier', 'subjectLabel', 'subject', 'type', 'applicableLevel'}

# 优先展示的字段（排在最前面）
PRIORITY_FIELDS = ['title', 'description']


def extract_types(pattern):
    """从元路径模式字符串提取类型序列"""
    return re.findall(r'\(:([^)]+)\)', pattern)


def format_value(v):
    """格式化字段值用于MD输出"""
    if isinstance(v, list):
        lines = []
        for item in v:
            s = str(item).strip()
            if s:
                lines.append(f"  - {s}")
        return '\n'.join(lines)
    return str(v)
    return s


def format_fields_md(props):
    """将节点属性（不含title）格式化为MD字段列表"""
    lines = []

    # 按优先级排序字段
    sorted_keys = []
    for k in PRIORITY_FIELDS:
        if k in props and k not in SKIP_FIELDS and props[k]:
            sorted_keys.append(k)
    other_keys = sorted(
        k for k in props
        if k not in SKIP_FIELDS and k not in sorted_keys and props[k]
    )
    sorted_keys.extend(other_keys)

    for k in sorted_keys:
        v = props[k]
        display_key = k[3:] if k.startswith('CJ_') else k
        val_str = format_value(v)
        if '\n' in val_str:
            lines.append(f"**{display_key}**:")
            lines.append(val_str)
            lines.append("")
        else:
            lines.append(f"**{display_key}**: {val_str}")

    return '\n'.join(lines)


def format_node_md(props, heading_level):
    """将一个结构节点格式化为MD段落"""
    lines = []
    title = props.get('title', '未知')
    ntype = props.get('type', '')
    prefix = '#' * min(heading_level, 6)

    # 标题中标注类型（便于区分）
    display_title = f"{prefix} {title}" if heading_level <= 4 else f"{prefix} {title}"
    lines.append(display_title)
    lines.append("")

    # 只展示非title字段
    field_props = {k: v for k, v in props.items() if k != 'title'}
    field_md = format_fields_md(field_props)
    if field_md.strip():
        lines.append(field_md)
        lines.append("")

    return '\n'.join(lines)


def format_metadata_inline(props):
    """将元数据节点格式化为内联展示（用于关联内容区域）"""
    title = props.get('title', '未知')
    ntype = props.get('type', '')

    lines = [f"- **[{ntype}] {title}**"]

    # 展示关键字段（跳过系统字段和title）
    for k, v in sorted(props.items()):
        if k in SKIP_FIELDS or k == 'title' or not v:
            continue
        display_key = k[3:] if k.startswith('CJ_') else k
        val_str = format_value(v)
        lines.append(f"  - **{display_key}**: {val_str}")

    return '\n'.join(lines)


def compute_type_level(metapaths):
    """根据元路径计算结构类型的标题层级"""
    type_level = {}
    for p in metapaths:
        types = extract_types(p)
        for i, t in enumerate(types):
            if t in STRUCTURAL_TYPES and t not in type_level:
                type_level[t] = i + 2  # ## 起步
    return type_level


def generate_subject_md(subject, metapaths, driver):
    """为单个学科生成MD内容"""
    if not metapaths:
        return None

    start_type = extract_types(metapaths[0])[0]

    # 计算结构类型层级
    type_level = compute_type_level(metapaths)

    # 从元路径提取所有合法的有向类型对
    valid_edges = set()
    for p in metapaths:
        types = extract_types(p)
        for i in range(len(types) - 1):
            valid_edges.add((types[i], types[i + 1]))

    # 收集涉及的所有类型
    all_types = set()
    for p in metapaths:
        all_types.update(extract_types(p))

    # 一次查询：获取该学科所有相关节点
    with driver.session() as session:
        node_result = session.run("""
            MATCH (n)
            WHERE n.subject = $subject AND n.type IN $types
            RETURN n.identifier AS id, properties(n) AS props
        """, subject=subject, types=list(all_types))
        nodes = {r['id']: r['props'] for r in node_result}

        # 一次查询：获取该学科所有边
        edge_result = session.run("""
            MATCH (a)-[r]-(b)
            WHERE a.subject = $subject AND b.subject = $subject
              AND a.type IN $types AND b.type IN $types
            RETURN DISTINCT a.identifier AS from_id, a.type AS from_type,
                           b.identifier AS to_id, b.type AS to_type
        """, subject=subject, types=list(all_types))

        # 分为结构邻接和元数据邻接
        structural_adj = defaultdict(set)   # parent_id -> structural child_ids
        metadata_adj = defaultdict(set)     # node_id -> metadata neighbor ids
        for rec in edge_result:
            from_type, to_type = rec['from_type'], rec['to_type']
            if (from_type, to_type) in valid_edges or (to_type, from_type) in valid_edges:
                from_id, to_id = rec['from_id'], rec['to_id']
                if to_type in STRUCTURAL_TYPES:
                    structural_adj[from_id].add(to_id)
                else:
                    metadata_adj[from_id].add(to_id)

    if not nodes:
        return None

    # 根节点按identifier排序
    root_ids = sorted(
        [nid for nid, p in nodes.items() if p.get('type') == start_type],
        key=lambda nid: nodes[nid].get('identifier', '')
    )

    if not root_ids:
        return None

    # DFS：只遍历结构层级，元数据内联展示
    md_lines = [f"# {subject}\n"]
    visited = set()

    def dfs(node_id, level):
        if node_id in visited:
            return
        visited.add(node_id)

        props = nodes.get(node_id, {})
        ntype = props.get('type', '')
        heading_level = type_level.get(ntype, min(level + 1, 6))

        md_lines.append(format_node_md(props, heading_level))

        # 展示该节点的元数据邻居（内联，不递归）
        meta_neighbors = metadata_adj.get(node_id, set())
        if meta_neighbors:
            md_lines.append(f"> **关联内容** ({len(meta_neighbors)} 项)")
            md_lines.append(">")
            # 按类型分组展示
            sorted_meta = sorted(
                meta_neighbors,
                key=lambda mid: (nodes.get(mid, {}).get('type', ''), nodes.get(mid, {}).get('title', ''))
            )
            for mid in sorted_meta:
                mprops = nodes.get(mid, {})
                if mprops:
                    md_lines.append(">" + format_metadata_inline(mprops))
            md_lines.append("")

        # 递归遍历结构子节点
        children = sorted(
            structural_adj.get(node_id, set()),
            key=lambda cid: (
                type_level.get(nodes.get(cid, {}).get('type', ''), 99),
                nodes.get(cid, {}).get('identifier', '')
            )
        )
        for child_id in children:
            dfs(child_id, heading_level)

    for rid in root_ids:
        dfs(rid, 2)

    return '\n'.join(md_lines)


if __name__ == '__main__':
    driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    try:
        for subject in SUBJECTS:
            print(f'Generating MD for {subject}...')
            metapath_file = os.path.join(METAPATH_DIR, f'{subject}.json')
            if not os.path.exists(metapath_file):
                print(f'  无元路径文件，跳过')
                continue

            with open(metapath_file, 'r', encoding='utf-8') as f:
                metapaths = json.load(f)

            if not metapaths:
                print(f'  元路径为空，跳过')
                continue

            content = generate_subject_md(subject, metapaths, driver)
            if content:
                filepath = os.path.join(OUTPUT_DIR, f'{subject}.md')
                with open(filepath, 'w', encoding='utf-8') as f:
                    f.write(content)
                print(f'  已保存到 {filepath} ({len(content)} 字符)')
            else:
                print(f'  无内容，跳过')
    finally:
        driver.close()

    print('\nDone!')
