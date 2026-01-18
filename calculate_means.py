import pandas as pd
import glob
from pathlib import Path
import os
import multiprocessing
import csv
import sys

# -----------------------------------------------------------------
# 1. 【!! 关键修复 !!】: 强行突破 CSV 读取限制
# -----------------------------------------------------------------
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)
print(f"!!! 已将 CSV 字段大小限制提高到 {max_int} 以处理损坏文件 !!!")
# -----------------------------------------------------------------


# -----------------------------------------------------------------
# 2. 【设置】
# -----------------------------------------------------------------
base_dir = Path("E:/dissertation/countclim-histsoc")
models = [
    "h08", "hydropy", "jules-w2", "lpjml5-7-10-fire",
    "miroc-integ-land", "watergap2-2e", "web-dhm-sg"
]

# 我们知道 R 脚本生成的正确列名
# (来自 R 脚本: data.table(Grid_ID, Lon, Lat, Date, Qtot, SCI))
CORRECT_COLUMN_NAMES = ['Grid_ID', 'Lon', 'Lat', 'Date', 'Qtot', 'SCI']

try:
    WORKER_COUNT = 2
    if WORKER_COUNT < 1: WORKER_COUNT = 1
except NotImplementedError:
    WORKER_COUNT = 4

print(f"--- ------------------------------------ ---")
print(f"--- 正在处理情景: {base_dir.name} (并行加速 + 错误修复 v4) ---")
print(f"--- 将使用 {WORKER_COUNT} 个 CPU 核心同时处理 ---")
print(f"--- ------------------------------------ ---")


# -----------------------------------------------------------------
# 3. 定义“单个工人”的任务
# -----------------------------------------------------------------
def process_batch(suffix):
    """
    此函数处理 *一个* 批次 (例如 'grids_1_160.csv')
    """
    print(f"  -- [开始] 正在处理批次: {suffix} --")

    batch_data_list = []

    for mod in models:
        file_path = base_dir / f"{mod}_{suffix}"

        if file_path.exists():
            try:
                # 【!! 此处是唯一的更改 !!】
                # 我们强行指定了列名，并跳过了损坏的标题行
                df = pd.read_csv(
                    file_path,
                    engine='python',
                    encoding='latin-1',
                    on_bad_lines='warn',  # 跳过坏行
                    header=None,  # <--- 新增：不读取标题行
                    skiprows=1,  # <--- 新增：跳过(损坏的)第1行
                    names=CORRECT_COLUMN_NAMES  # <--- 新增：强行使用此列名
                )

                # 强制将列转换为数字
                df['SCI'] = pd.to_numeric(df['SCI'], errors='coerce')
                df['Grid_ID'] = pd.to_numeric(df['Grid_ID'], errors='coerce')
                df['Lon'] = pd.to_numeric(df['Lon'], errors='coerce')
                df['Lat'] = pd.to_numeric(df['Lat'], errors='coerce')

                # 我们只保留我们需要（且存在）的列
                columns_to_keep = ['Grid_ID', 'Lon', 'Lat', 'Date', 'SCI']
                existing_cols = [col for col in columns_to_keep if col in df.columns]
                df_subset = df[existing_cols]
                batch_data_list.append(df_subset)

            except Exception as e:
                # 捕获其他可能的错误
                print(f"  !! 警告: 读取文件 {file_path} 失败: {e}")
        else:
            print(f"  !! 警告: 模型 {mod} 缺少批次 {suffix}，已跳过。")

    if not batch_data_list:
        print(f"  !! 警告: 批次 {suffix} 未加载到任何数据，跳过。")
        return False  # 返回失败

    # 合并、计算均值、保存
    try:
        batch_all_models_data = pd.concat(batch_data_list, ignore_index=True)
        del batch_data_list

        group_keys = ['Grid_ID', 'Lon', 'Lat', 'Date']
        valid_group_keys = [key for key in group_keys if key in batch_all_models_data.columns]

        mean_batch_data = batch_all_models_data.groupby(
            valid_group_keys
        ).agg(
            Mean_SCI=('SCI', 'mean')
        ).reset_index()

        temp_dir = base_dir / "TEMP_MEANS"
        temp_dir.mkdir(exist_ok=True)

        temp_output_file = temp_dir / f"TEMP_MEAN_{suffix}"
        mean_batch_data.to_csv(temp_output_file, index=False, float_format='%.6f')

        print(f"  -- [完成] 批次 {suffix} 处理完毕。 --")
        return True  # 返回成功

    except Exception as e:
        print(f"  !! 严重错误: 处理批次 {suffix} 时失败: {e}")
        return False  # 返回失败


# -----------------------------------------------------------------
# 4. 【主程序】: 创建“工头”并分配任务
# -----------------------------------------------------------------
if __name__ == "__main__":

    all_suffixes = set()
    for mod in models:
        files = base_dir.glob(f"{mod}_grids_*.csv")
        suffixes = [f.name.replace(f"{mod}_", "") for f in files]
        all_suffixes.update(suffixes)

    if not all_suffixes:
        print(f"!! 严重错误: 在 {base_dir} 中未找到任何模型的任何批次文件。")
        exit()

    batch_suffixes = sorted(list(all_suffixes))
    print(f"找到 {len(batch_suffixes)} 个独特的批次后缀。开始分派任务...")

    with multiprocessing.Pool(processes=WORKER_COUNT) as pool:
        results = pool.map(process_batch, batch_suffixes)

    print(f"\n====================================================")
    print(f"--- 所有 {len(batch_suffixes)} 个批次均已处理完毕 ---")

    success_count = sum(results)
    print(f"--- 成功: {success_count} 个批次 ---")
    if len(batch_suffixes) - success_count > 0:
        print(f"--- 失败: {len(batch_suffixes) - success_count} 个批次 ---")

    print(f"--- 您的 {success_count} 个临时均值文件位于: {base_dir / 'TEMP_MEANS'} ---")
    print("====================================================")