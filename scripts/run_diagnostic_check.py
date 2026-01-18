import pandas as pd
from pathlib import Path

# -----------------------------------------------------------------
# 1. 【设置】
# (！！请仔细检查这两个文件的路径！！)
# -----------------------------------------------------------------

# “坐标”情景 (完整的)
coord_file = Path("F:/fyp/obsclim-histsoc/obsclim-histsoc_RATIO_STATS.csv")

# “数据”情景 (有缺口的)
data_file = Path("F:/fyp/countclim-histsoc/countclim-histsoc_RATIO_STATS.csv")

print("--- ---------------------------------- ---")
print("--- 正在运行数据完整性诊断检查 ---")
print("--- ---------------------------------- ---")

try:
    # 1. 加载文件并获取 Grid_ID
    if not coord_file.exists():
        print(f"!! 错误: 找不到“完整”文件: {coord_file}")
        exit()
    df_good = pd.read_csv(coord_file)
    good_grids = set(df_good['Grid_ID'])
    print(f"“完整”文件 ({coord_file.name}) 包含: {len(good_grids)} 个独特的 Grid_ID。")

    if not data_file.exists():
        print(f"!! 错误: 找不到“有缺口”文件: {data_file}")
        exit()
    df_bad = pd.read_csv(data_file)
    bad_grids = set(df_bad['Grid_ID'])
    print(f"“有缺口”文件 ({data_file.name}) 包含: {len(bad_grids)} 个独特的 Grid_ID。")

    # 2. 比较两个集合
    print("\n--- 诊断结果 ---")

    missing_grids = good_grids - bad_grids  # 在“完整”中但不在“有缺口”中

    if not missing_grids:
        print("!! 诊断失败: 两个文件包含完全相同的 Grid_ID。")
        print("!! 这非常奇怪。请联系技术支持。")
    else:
        print(f"!! 诊断成功：'{data_file.name}' 文件中缺失了 {len(missing_grids)} 个 Grid_ID。")
        print("缺失的 Grid_ID 列表 (仅显示前 20 个):")
        print(list(missing_grids)[:20])

        # 3. 找出这些缺失的 Grid_ID 属于哪个批次
        print("\n--- 解决方案 ---")
        print("这些缺失的 Grid_ID 位于以下坐标范围 (及对应的批次文件):")

        # 找出缺失的网格在“完整”文件中的坐标
        missing_grid_details = df_good[df_good['Grid_ID'].isin(missing_grids)]

        # 找出缺失的批次
        # (假设 160 个网格为一批)
        missing_grid_details['Batch_Group'] = (missing_grid_details['Grid_ID'] - 1) // 160

        batches = missing_grid_details['Batch_Group'].unique()

        for batch_num in sorted(batches):
            start_grid = (batch_num * 160) + 1
            end_grid = (batch_num + 1) * 160
            batch_suffix = f"grids_{start_grid}_{end_grid}.csv"
            print(f"  - 批次: {batch_suffix}")

        print(f"\n请您去检查 'F:\\fyp\\countclim-histsoc' 文件夹，")
        print(f"确认该文件夹中**是否缺少**上述批次的**所有 7 个模型**的原始 .csv 文件。")
        print("您必须重新运行 R 脚本为 'countclim-histsoc' 生成这些缺失的批次文件。")

except Exception as e:
    print(f"!! 诊断时发生错误: {e}")