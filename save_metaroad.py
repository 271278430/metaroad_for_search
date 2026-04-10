import json, os
from meta_road import MetaPathMiner

subjects = [
    '义教化学', '义教历史', '义教地理', '义教数学', '义教物理', '义教生物', '义教英语', '义教语文', '义教道法',
    '高中化学', '高中历史', '高中地理', '高中政治', '高中数学', '高中物理', '高中生物', '高中英语', '高中语文',
]

output_dir = './metapaths'
os.makedirs(output_dir, exist_ok=True)

miner = MetaPathMiner(
    uri='bolt://10.50.243.143:7687',
    user='neo4j',
    password='neo4j123',
    subject_property='subject',
    type_property='type'
)

try:
    for subject in subjects:
        safe_name = subject.replace(' ', '_')
        filepath = os.path.join(output_dir, f'{safe_name}.json')

        # 读取已有路径
        existing = set() #哈希思想
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as f:
                old = json.load(f)
                existing = set(old)

        # 先用 Chapter，如果结果为空则回退到 Unit
        patterns = miner.mine_metapaths_raw(max_depth=10, start_type='Chapter', subject=subject)
        start_type_used = 'Chapter'
        if len(patterns) == 0:
            print(f'{subject}: Chapter 无结果，尝试 Unit...')
            patterns = miner.mine_metapaths_raw(max_depth=10, start_type='Unit', subject=subject)
            start_type_used = 'Unit'

        # 去重：只添加不存在的新路径
        new_patterns = [p for p in patterns if p not in existing]
        all_patterns = list(existing) + new_patterns

        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(all_patterns, f, ensure_ascii=False, indent=2)

        print(f'{subject} (start={start_type_used}): 新增 {len(new_patterns)}, 总计 {len(all_patterns)}')
finally:
    miner.close()

print('\nDone!')
