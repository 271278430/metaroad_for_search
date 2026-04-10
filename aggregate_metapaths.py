import re


class TrieNode:
    def __init__(self, type_name=None):
        self.children = {}
        self.type_name = type_name
        self.var_name = None


def extract_types(pattern):
    """从模式字符串中提取类型序列"""
    return re.findall(r'\(:([^)]+)\)', pattern)


def build_trie(type_sequences):
    """根据类型序列列表构建前缀树"""
    root = TrieNode()
    for types in type_sequences:
        node = root
        for t in types:
            if t not in node.children:
                node.children[t] = TrieNode(t)
            node = node.children[t]
    return root


def assign_vars(root):
    """给每个节点分配变量名"""
    counter = [0]
    def dfs(node):
        node.var_name = f"n{counter[0]}"
        counter[0] += 1
        for child in node.children.values():
            dfs(child)
    dfs(root)


def _collect_segments(root):
    """
    从前缀树收集所有线性和分支片段。
    线性路径合并为一条，分支处拆分为独立的模式片段。
    """
    segments = []

    def dfs(node, current_segment):
        if node.type_name is not None:
            current_segment.append(node)

        if len(node.children) == 0:
            segments.append(list(current_segment))
        elif len(node.children) == 1:
            child = list(node.children.values())[0]
            dfs(child, current_segment)
        else:
            segments.append(list(current_segment))
            for child in node.children.values():
                dfs(child, [node])

    for child in root.children.values():
        dfs(child, [])

    return segments


def _segment_to_pattern(seg):
    """将片段转为 Cypher 模式字符串（不带标签，用属性过滤）"""
    parts = [f"({n.var_name})" for n in seg]
    return "-[]-".join(parts)


def _collect_all_nodes(root):
    """收集前缀树中所有节点"""
    all_nodes = []
    def dfs(node):
        if node.type_name is not None:
            all_nodes.append(node)
        for child in node.children.values():
            dfs(child)
    dfs(root)
    return all_nodes


def _build_where(all_nodes, subject_property=None, subject_value=None, type_property="type"):
    """生成 WHERE 子句：包含 type 属性约束和可选的 subject 约束"""
    conditions = [f"{n.var_name}.{type_property} = '{n.type_name}'" for n in all_nodes]
    if subject_property and subject_value:
        conditions += [f"{n.var_name}.{subject_property} = '{subject_value}'" for n in all_nodes]
    return "WHERE " + " AND ".join(conditions)


def generate_cypher_match(root, subject_property=None, subject_value=None):
    """
    生成 MATCH 聚合语句（AND 语义：所有分支必须同时存在）。
    适合用于展示树形结构。
    """
    segments = _collect_segments(root)
    clauses = [_segment_to_pattern(seg) for seg in segments]
    result = "MATCH " + ",\n      ".join(clauses)

    all_nodes = _collect_all_nodes(root)
    result += "\n" + _build_where(all_nodes, subject_property, subject_value)

    return result


def generate_cypher_optional_match(root, subject_property=None, subject_value=None):
    """
    生成 OPTIONAL MATCH 聚合语句（OR 语义：各分支独立匹配，可部分缺失）。
    适合用于实际匹配和验证，缺失分支返回 NULL。
    """
    segments = _collect_segments(root)

    if not segments:
        return ""

    # 第一条是主干，用 MATCH
    lines = ["MATCH " + _segment_to_pattern(segments[0])]

    # 后续都是分支，用 OPTIONAL MATCH，从分支点开始
    for seg in segments[1:]:
        lines.append("OPTIONAL MATCH " + _segment_to_pattern(seg))

    # WHERE 子句：type 属性 + 可选 subject 属性
    all_nodes = _collect_all_nodes(root)
    lines.append(_build_where(all_nodes, subject_property, subject_value))

    # 生成 RETURN 子句
    return_items = [f"{n.var_name} IS NOT NULL AS {n.var_name}_{n.type_name}" for n in all_nodes]

    lines.append("RETURN")
    lines.append("      " + ",\n      ".join(return_items))

    return "\n".join(lines)


def generate_cypher_union(patterns, subject_property=None, subject_value=None):
    """
    生成 UNION ALL 聚合语句：每条元路径独立查询，结果合并。
    不会产生笛卡尔积，内存安全。
    """
    clauses = []

    for pattern in patterns:
        types = extract_types(pattern)
        aliases = [f"n{i}" for i in range(len(types))]

        # 构建 MATCH path = (n0)-[]-(n1)-[]-...
        path_str = f"({aliases[0]})"
        for i in range(1, len(aliases)):
            path_str += f"-[]-({aliases[i]})"

        # 构建 WHERE
        conditions = [f"{aliases[i]}.type = '{types[i]}'" for i in range(len(types))]
        if subject_property and subject_value:
            conditions += [f"{aliases[i]}.{subject_property} = '{subject_value}'" for i in range(len(types))]

        clause = f"MATCH path = {path_str}\nWHERE {' AND '.join(conditions)}\nRETURN path"
        clauses.append(clause)

    return "\nUNION ALL\n".join(clauses)



    """打印前缀树的可视化结构"""
    def dfs(node, prefix, is_last):
        if node.type_name is not None:
            connector = "└ " if is_last else "├ "
            print(prefix + connector + node.type_name)
            prefix += "   " if is_last else "│  "

        children = list(node.children.values())
        for i, child in enumerate(children):
            dfs(child, prefix, i == len(children) - 1)

    children = list(root.children.values())
    for i, child in enumerate(children):
        dfs(child, "", i == len(children) - 1)


def render_tree_image(root, output_path="metapath_tree.png"):
    """使用 graphviz 生成树形可视化图片"""
    import graphviz

    dot = graphviz.Digraph(comment="Metapath Tree")
    dot.attr(rankdir="TB", dpi="150")
    dot.attr("node", shape="box", style="rounded,filled", fillcolor="#E8F0FE",
             fontname="Arial", fontsize="11", margin="0.15,0.1")
    dot.attr("edge", color="#5B9BD5", penwidth="1.5")

    # 给每个节点分配一个唯一的 dot_id
    node_id_counter = [0]
    def get_dot_id(node):
        if not hasattr(node, '_dot_id'):
            node._dot_id = f"node_{node_id_counter[0]}"
            node_id_counter[0] += 1
        return node._dot_id

    def dfs(node, parent_dot_id=None):
        if node.type_name is not None:
            dot_id = get_dot_id(node)
            label = node.type_name
            # 叶节点用不同颜色
            if not node.children:
                dot.node(dot_id, label=label, fillcolor="#D5E8D4", fontcolor="#1B5E20")
            # 分支点用不同颜色
            elif len(node.children) > 1:
                dot.node(dot_id, label=label, fillcolor="#FFF2CC", fontcolor="#7F6000")
            else:
                dot.node(dot_id, label=label)

            if parent_dot_id is not None:
                dot.edge(parent_dot_id, dot_id)
        else:
            dot_id = parent_dot_id  # root 节点没有 type

        for child in node.children.values():
            dfs(child, dot_id)

    # 根节点不可见，从其子节点开始
    for child in root.children.values():
        dfs(child, None)

    dot.render(output_path.replace(".png", ""), format="png", cleanup=True)
    print(f"树形可视化图片已保存到: {output_path}")


if __name__ == "__main__":
    import sys

    # 手动填写的元路径列表
    manual_patterns = [
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)-[]-(:CourseNature)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)-[]-(:CourseNature)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:SubSection)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)-[]-(:CourseNature)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:Implementation)-[]-(:CoreLiteracy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)",
    "(:Chapter)-[]-(:Section)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:Implementation)-[]-(:CoreLiteracy)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:Implementation)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:Implementation)",
]



    if len(sys.argv) > 1 and sys.argv[1] == "--auto":
        # 自动从 meta_road.py 获取
        from meta_road import MetaPathMiner
        miner = MetaPathMiner(
            uri="bolt://10.50.243.143:7687",
            user="neo4j",
            password="neo4j123",
            subject_property="subject",
            type_property="type"
        )
        try:
            patterns = miner.mine_metapaths_raw(max_depth=10, start_types="Chapter", subject="高中数学")
        finally:
            miner.close()
    else:
        patterns = manual_patterns

    # 构建前缀树
    type_sequences = [extract_types(p) for p in patterns]
    root = build_trie(type_sequences)
    assign_vars(root)

    # 生成树形可视化图片
    print("\n" + "=" * 60)
    render_tree_image(root, output_path="/data/shanghui/meta_road/metapath_tree.png")

    # 学科属性约束
    subject_prop = "subject"
    subject_val = "高中数学"

    # # 生成 MATCH 聚合 Cypher（AND 语义）
    # print("\n" + "=" * 60)
    # print("MATCH 聚合语句（AND 语义，所有分支必须同时存在）:")
    # print("=" * 60)
    # print(generate_cypher_match(root, subject_prop, subject_val))

    # # 生成 OPTIONAL MATCH 聚合 Cypher（OR 语义）
    # print("\n" + "=" * 60)
    # print("OPTIONAL MATCH 聚合语句（OR 语义，各分支独立匹配）:")
    # print("=" * 60)
    # print(generate_cypher_optional_match(root, subject_prop, subject_val))

    # 生成 UNION ALL 聚合 Cypher（每条路径独立查询，结果合并）
    print("\n" + "=" * 60)
    print("UNION ALL 聚合语句（每条路径独立查询，结果合并）:")
    print("=" * 60)
    print(generate_cypher_union(patterns, subject_prop, subject_val))
