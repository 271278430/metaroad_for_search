from neo4j import GraphDatabase
from typing import List, Tuple, Optional, Set, Generator

class MetaPathMiner:
    """
    自动挖掘学科图谱中所有无环、节点类型不重复的元路径，并输出Cypher模式字符串。
    节点类型由节点的 type 属性标识，学科由 subject 属性标识。
    输出格式：(:TypeA)-[]-(:TypeB)
    """

    def __init__(self, uri: str, user: str, password: str, subject_property: str = "subject", type_property: str = "type"):
        """
        初始化Neo4j连接驱动。

        :param uri: Neo4j Bolt连接地址，例如 "bolt://localhost:7687"
        :param user: 数据库用户名
        :param password: 数据库密码
        :param subject_property: 节点上表示学科的属性名（默认 "subject"）
        :param type_property: 节点上表示实体类型的属性名（默认 "type"）
        """
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.neighbor_cache = {}
        self.subject_cache = {}
        self.subject_property = subject_property
        self.type_property = type_property

    def close(self):
        """关闭数据库连接，释放资源。"""
        self.driver.close()

    def _get_all_types(self) -> List[str]:
        """
        获取图中所有不同的节点 type 属性值（实体类型）。

        :return: 实体类型名称列表，例如 ["Chapter", "Section", "Course", "Teacher"]
        """
        with self.driver.session() as session:
            result = session.run(
                f"MATCH (n) WHERE n.{self.type_property} IS NOT NULL RETURN DISTINCT n.{self.type_property} AS type"
            )
            return [record["type"] for record in result]

    def _get_start_nodes(self, entity_type: str, limit: int = 10000, subject: Optional[str] = None) -> List[str]:
        """
        获取指定实体类型下的节点 elementId 列表，作为DFS起始节点。支持按学科过滤。

        :param entity_type: 实体类型（type 属性值）
        :param limit: 最多返回的节点数量（性能考虑）
        :param subject: 可选，限定节点的subject属性值
        :return: 节点 elementId 列表
        """
        with self.driver.session() as session:
            where_clauses = [f"n.{self.type_property} = $entity_type"]
            params = {"entity_type": entity_type, "limit": limit}
            if subject is not None:
                where_clauses.append(f"n.{self.subject_property} = $subject")
                params["subject"] = subject

            query = f"""
                MATCH (n)
                WHERE {' AND '.join(where_clauses)}
                RETURN elementId(n) AS element_id
                LIMIT $limit
            """
            result = session.run(query, **params)
            return [record["element_id"] for record in result]

    def _get_neighbors(self, node_element_id: str) -> List[Tuple[str, str]]:
        """
        获取一个节点的所有邻居（无向），并附带邻居的 type 属性。

        使用缓存避免重复查询同一节点的邻居。

        :param node_element_id: 当前节点的 elementId
        :return: 邻居列表，每个元素为 (邻居elementId, 邻居type属性值)
        """
        if node_element_id in self.neighbor_cache:
            return self.neighbor_cache[node_element_id]

        neighbors = []
        with self.driver.session() as session:
            # 无向匹配，忽略方向
            result = session.run(
                """
                MATCH (n)-[r]-(m)
                WHERE elementId(n) = $node_element_id
                RETURN elementId(m) AS neighbor_element_id,
                       m.`type` AS neighbor_type
                """,
                node_element_id=node_element_id
            )
            for record in result:
                neighbor_type = record["neighbor_type"]
                if neighbor_type is not None:
                    neighbors.append((
                        record["neighbor_element_id"],
                        neighbor_type
                    ))

        self.neighbor_cache[node_element_id] = neighbors
        return neighbors

    def _dfs(self,
             current_node_id: str,
             current_path_nodes: List[str],
             current_path_types: List[str],
             max_depth: int,
             subject: Optional[str] = None) -> Generator[str, None, None]:
        """
        深度优先搜索递归生成所有元路径，返回Cypher模式字符串。

        :param current_node_id: 当前节点的 elementId
        :param current_path_nodes: 当前已访问的节点 elementId 列表（用于判环）
        :param current_path_types: 当前已访问的节点 type 属性值列表
        :param max_depth: 最大允许的节点数（路径长度）
        :param subject: 可选，限定节点的subject属性值（非空时，路径中所有节点必须满足该属性）
        :yield: 一条元路径的Cypher模式字符串，如 "(:Chapter)-[]-(:Section)"
        """
        if len(current_path_types) >= max_depth:
            return

        neighbors = self._get_neighbors(current_node_id)

        for neighbor_id, neighbor_type in neighbors:
            # 1. 避免环路（节点重复）
            if neighbor_id in current_path_nodes:
                continue

            # 2. 避免实体类型重复（type属性值重复）
            if neighbor_type in current_path_types:
                continue

            # 3. 如果限定了学科，则必须检查邻居节点的 subject 属性
            if subject is not None:
                if neighbor_id not in self.subject_cache:
                    with self.driver.session() as session:
                        result = session.run(
                            f"MATCH (n) WHERE elementId(n) = $node_id RETURN n.{self.subject_property} AS subject_val",
                            node_id=neighbor_id
                        )
                        record = result.single()
                        result.consume()
                        if record is None:
                            continue
                        self.subject_cache[neighbor_id] = record["subject_val"]
                neighbor_subject = self.subject_cache[neighbor_id]
                if neighbor_subject != subject:
                    continue

            new_path_nodes = current_path_nodes + [neighbor_id]
            new_path_types = current_path_types + [neighbor_type]

            cypher_pattern = self._build_pattern(new_path_types)
            yield cypher_pattern

            yield from self._dfs(
                neighbor_id,
                new_path_nodes,
                new_path_types,
                max_depth,
                subject
            )

    def _build_pattern(self, types: List[str]) -> str:
        """
        将节点类型序列组合成Cypher无向模式字符串。
        节点表示为 (:TypeValue) 形式，使用节点的 type 属性值作为标签。

        :param types: 节点 type 属性值列表，长度至少为1
        :return: 例如 "(:Chapter)-[]-(:Section)"
        """
        if not types:
            return ""
        pattern = f"(:{types[0]})"
        for i in range(1, len(types)):
            pattern += f"-[]-(:{types[i]})"
        return pattern

    @staticmethod
    def _extract_types(pattern: str) -> List[str]:
        """从模式字符串中提取类型序列，如 '(:A)-[]-(:B)-[]-(:C)' -> ['A', 'B', 'C']"""
        import re
        return re.findall(r'\(:([^)]+)\)', pattern)

    @staticmethod
    def _is_subpath(short: List[str], long: List[str]) -> bool:
        """判断 short 是否是 long 的连续子序列"""
        short_len = len(short)
        for start in range(len(long) - short_len + 1):
            if long[start:start + short_len] == short:
                return True
        return False

    def remove_redundant(self, patterns: List[str]) -> List[str]:
        """
        移除被更长路径包含的子路径（支持无向，即也检查反向序列）。
        如果路径 B 的类型序列或其反转是路径 A 的连续子序列，则移除 B。

        逻辑是：
        提取类型序列 — 把每条路径解析成类型列表，如 (:A)-[]-(:B)-[]-(:C) → [A, B, C]
        两两比较 — 对每条路径 i，遍历所有其他路径 j
        判断包含关系 — 如果路径 i 比路径 j 短，检查：
        i 的类型序列是否是 j 的连续子序列（正向匹配）
        i 的类型序列的反转是否是 j 的连续子序列（反向匹配，处理无向路径）
        任一成立 → i 是 j 的子路径
        标记移除 — 被包含的短路径标记为不保留
        """
        type_seqs = [self._extract_types(p) for p in patterns]
        n = len(patterns)
        keep = [True] * n

        for i in range(n):
            if not keep[i]:
                continue
            for j in range(n):
                if i == j or not keep[j]:
                    continue
                if len(type_seqs[i]) <= len(type_seqs[j]):
                    seq_i = type_seqs[i]
                    seq_j = type_seqs[j]
                    # 检查正向和反向
                    if (self._is_subpath(seq_i, seq_j) or
                        self._is_subpath(list(reversed(seq_i)), seq_j)):
                        keep[i] = False
                        break

        return [patterns[i] for i in range(n) if keep[i]]


        """
        将节点类型序列组合成Cypher无向模式字符串。
        节点表示为 (:TypeValue) 形式，使用节点的 type 属性值作为标签。

        :param types: 节点 type 属性值列表，长度至少为1
        :return: 例如 "(:Chapter)-[]-(:Section)"
        """
        if not types:
            return ""
        pattern = f"(:{types[0]})"
        for i in range(1, len(types)):
            pattern += f"-[]-(:{types[i]})"
        return pattern

    def mine_metapaths(self,
                       max_depth: int = 5,
                       start_type: Optional[str] = None,
                       limit_per_type: int = 1000,
                       subject: Optional[str] = None) -> List[str]:
        """
        主入口：挖掘所有元路径并返回Cypher模式列表。

        :param max_depth: 最大路径节点数（长度），默认5，避免组合爆炸
        :param start_type: 可选，仅从指定实体类型（type属性值）的节点开始搜索。若为None则从所有类型开始
        :param limit_per_type: 每个实体类型最多取多少个起始节点（性能控制）
        :param subject: 可选，学科名称。若提供，则只搜索subject属性等于该值的节点子图（严格模式）
        :return: 去重后的元路径Cypher模式列表
        """
        # 确定要处理的起始实体类型列表
        if start_type:
            all_types = [start_type]
        else:
            all_types = self._get_all_types()

        # 存储起始节点：按类型分组
        start_nodes_by_type = {}
        for entity_type in all_types:
            nodes = self._get_start_nodes(entity_type, limit=limit_per_type, subject=subject)
            if nodes:
                start_nodes_by_type[entity_type] = nodes

        metapath_patterns: Set[str] = set()

        for entity_type, start_nodes in start_nodes_by_type.items():
            print(f"Processing start type: {entity_type} (nodes: {len(start_nodes)})")
            for node_id in start_nodes:
                current_path_nodes = [node_id]
                current_path_types = [entity_type]

                for pattern in self._dfs(
                    node_id,
                    current_path_nodes,
                    current_path_types,
                    max_depth,
                    subject
                ):
                    metapath_patterns.add(pattern)

        return self.remove_redundant(list(metapath_patterns))

    def mine_metapaths_raw(self,
                           max_depth: int = 5,
                           start_type: Optional[str] = None,
                           limit_per_type: int = 1000,
                           subject: Optional[str] = None) -> List[str]:
        """
        同 mine_metapaths 但不做子路径去重，保留所有长度的元路径。
        适用于 UNION ALL 聚合查询，确保覆盖所有节点。
        """
        if start_type:
            all_types = [start_type]
        else:
            all_types = self._get_all_types()

        start_nodes_by_type = {}
        for entity_type in all_types:
            nodes = self._get_start_nodes(entity_type, limit=limit_per_type, subject=subject)
            if nodes:
                start_nodes_by_type[entity_type] = nodes

        metapath_patterns: Set[str] = set()

        for entity_type, start_nodes in start_nodes_by_type.items():
            for node_id in start_nodes:
                current_path_nodes = [node_id]
                current_path_types = [entity_type]

                for pattern in self._dfs(
                    node_id,
                    current_path_nodes,
                    current_path_types,
                    max_depth,
                    subject
                ):
                    metapath_patterns.add(pattern)

        return list(metapath_patterns)


# ==================== 使用示例 ====================
if __name__ == "__main__":
    miner = MetaPathMiner(
        uri="bolt://10.50.243.143:7687",
        user="neo4j",
        password="neo4j123",
        subject_property="subject",   # 你的学科属性名
        type_property="type"          # 你的实体类型属性名
    )

    try:
        # all_types = miner._get_all_types()
        # print("图中所有 type 值:")
        # for t in all_types:
        #     print(f"  {t}")
        
        #设置了subject参数
        subject_name = "高中化学"

        # patterns_math = miner.mine_metapaths(
        #     max_depth=10,
        #     start_type="Chapter",  # 可以指定一个常见的起始类型，或者去除了start_type参数让它从所有类型开始
        #     subject=subject_name)
        # print(f"{subject_name} 学科元路径数: {len(patterns_math)}")
        # for p in patterns_math:
        #     print(p)

        patterns_math = miner.mine_metapaths_raw(
            max_depth=10,
            start_type="Chapter",  # 可以指定一个常见的起始类型，或者去除了start_type参数让它从所有类型开始
            subject=subject_name)
        print(f"{subject_name} 学科元路径数: {len(patterns_math)}")
        for p in patterns_math:
            print(p)
    finally:
        miner.close()