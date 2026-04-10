from neo4j import GraphDatabase


def validate_metapaths(uri: str, user: str, password: str, patterns: list):
    driver = GraphDatabase.driver(uri, auth=(user, password))

    try:
        matched = []
        unmatched = []

        for pattern in patterns:
            query = f"MATCH path = {pattern} RETURN path LIMIT 1"
            with driver.session() as session:
                """
                session.run(query) 返回的不是数据本身，而是一个 Result 对象（相当于一个游标/流）。
                数据还在数据库的缓冲区里，没有被取出来。
                都是从流中拉取数据，拉取后缓冲区就被消费了。

                result.single() — 取一条，其余丢弃
                result.data() — 取全部记录，全部消费
                for record in result: — 逐条遍历，逐条消费
                """
                result = session.run(query)
                record = result.single()
                """
                取完之后再调用 result.consume() 只是保险起见清理残留的元数据（查询摘要、通知等）。
                如果你已经用 single() 或 data() 取完了所有数据，consume() 其实不做什么额外工作。
                """
                result.consume()
                if record and record["path"]:
                    """
                    取决于你 RETURN 了什么。这里是 RETURN path，所以 record 只有一个字段：
                    record["path"] — 匹配到的路径对象（包含节点和关系的序列）

                    如果你写 RETURN path, count(*) AS cnt，那就有两个字段：
                    record["path"]
                    record["cnt"]
                    返回字段完全由 RETURN 子句决定，不是固定的。
                    """
                    matched.append(pattern)
                else:
                    unmatched.append(pattern)

        print(f"验证完成: 总计 {len(patterns)} 条元路径")
        print(f"  匹配成功: {len(matched)} 条")
        print(f"  匹配失败: {len(unmatched)} 条")

        if unmatched:
            print("\n以下元路径在数据库中未匹配到数据:")
            for p in unmatched:
                print(f"  ✗ {p}")

    finally:
        driver.close()


if __name__ == "__main__":
    import sys
    """
    两种用法：

    自动模式（直接从数据库挖掘+验证）：
    python3 validate_metapaths.py --auto

    手动模式（粘贴 patterns 列表）：
    python3 validate_metapaths.py
    """

    if len(sys.argv) > 1 and sys.argv[1] == "--auto":
        # 自动模式：直接从 meta_road.py 获取元路径
        from meta_road import MetaPathMiner

        miner = MetaPathMiner(
            uri="bolt://10.50.243.143:7687",
            user="neo4j",
            password="neo4j123",
            subject_property="subject",
            type_property="type"
        )
        try:
            patterns = miner.mine_metapaths(max_depth=10, subject="高中数学")
            print(f"自动获取到 {len(patterns)} 条元路径，开始验证...\n")
        finally:
            miner.close()

        validate_metapaths(
            uri="bolt://10.50.243.143:7687",
            user="neo4j",
            password="neo4j123",
            patterns=patterns,
        )
    else:
        # 手动模式：使用下方手动填写的 patterns 列表
        patterns = [
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)-[]-(:CourseNature)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:SubSection)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:Implementation)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)-[]-(:CourseNature)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:CoreLiteracy)-[]-(:Implementation)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)-[]-(:CourseNature)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:Implementation)-[]-(:CoreLiteracy)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:CoreLiteracy)-[]-(:AcademicQuality)-[]-(:CourseModule)-[]-(:Topic)",
    "(:Chapter)-[]-(:Section)-[]-(:SubSection)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:Domain)",
    "(:Chapter)-[]-(:Section)-[]-(:KeyPoint)-[]-(:Unit)-[]-(:Theme)-[]-(:CourseModule)-[]-(:AcademicQuality)-[]-(:Implementation)-[]-(:CoreLiteracy)-[]-(:CoursePhilosophy)",
]

        validate_metapaths(
            uri="bolt://localhost:7687",
            user="neo4j",
            password="neo4j123",
            patterns=patterns,
        )
