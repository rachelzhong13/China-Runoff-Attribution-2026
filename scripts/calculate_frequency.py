import pandas as pd
import glob
from pathlib import Path
import os
import csv
import sys

# -----------------------------------------------------------------
# 1. 【设置】
# (已为您的 'obsclim-histsoc' 情景更新)
# -----------------------------------------------------------------

# 您的父目录，即情景目录
base_dir = Path("F:/fyp/obsclim-histsoc")

# 您的情景名称 (用于输出文件名)
scenario_name = "obsclim-histsoc"

# 临时均值文件所在的文件夹
temp_mean_dir = base_dir / "TEMP_MEANS"

# 临时统计文件将要保存的文件夹
temp_stats_dir = base_dir / "TEMP_STATS"
temp_stats_dir.mkdir(exist_ok=True)  # 创建 'TEMP_STATS' 文件夹

# 定义干旱和洪涝的阈值
thresholds = {
    "Drought_1.0": (lambda x: x <= -1.0),
    "Drought_1.5": (lambda x: x <= -1.5),
    "Flood_1.0": (lambda x: x >= 1.0),
    "Flood_1.5": (lambda x: x >= 1.5)
}

print(f"--- ----------------------------------------- ---")
print(f"--- 正在为 {scenario_name} 计算干旱/洪涝频率 ---")
print(f"--- ----------------------------------------- ---")

# -----------------------------------------------------------------
# 2. 按批次计算频率
# -----------------------------------------------------------------
max_int = sys.maxsize
while True:
    try:
        csv.field_size_limit(max_int)
        break
    except OverflowError:
        max_int = int(max_int / 10)

batch_mean_files = glob.glob(str(temp_mean_dir / "TEMP_MEAN_*.csv"))

if not batch_mean_files:
    print(f"!! 严重错误: 在 {temp_mean_dir} 中未找到任何 TEMP_MEAN_ 文件。")
    print("!! 请先运行 'calculate_means.py' 脚本。")
    exit()

print(f"找到 {len(batch_mean_files)} 个均值批次文件。开始计算频率...")

temp_stat_files = []

for mean_file_path in batch_mean_files:

    file_name = Path(mean_file_path).name
    print(f"  -- 正在处理: {file_name} --")

    try:
        # 使用 'latin-1' 编码来读取，并跳过可能的损坏行
        df = pd.read_csv(mean_file_path, engine='python', encoding='latin-1', on_bad_lines='warn')

        grouped = df.groupby('Grid_ID')
        batch_stats_list = []

        for thresh_name, thresh_func in thresholds.items():
            # (使用 include_groups=False 消除 FutureWarning)
            freq_counts = grouped.apply(lambda x: thresh_func(x['Mean_SCI']).sum(), include_groups=False).reset_index(
                name=thresh_name)

            freq_counts = freq_counts.set_index('Grid_ID')
            batch_stats_list.append(freq_counts)

        batch_final_stats = pd.concat(batch_stats_list, axis=1)

        # 获取 Lon/Lat 信息
        grid_info = df[['Grid_ID', 'Lon', 'Lat']].drop_duplicates().set_index('Grid_ID')

        # 将 Lon/Lat 添加到统计结果中
        batch_final_stats_with_coords = grid_info.join(batch_final_stats).reset_index()

        # 步骤 3: 保存 *临时的* 统计文件
        temp_stat_file = temp_stats_dir / f"STAT_{file_name.replace('TEMP_MEAN_', '')}"
        batch_final_stats_with_coords.to_csv(temp_stat_file, index=False)
        temp_stat_files.append(temp_stat_file)

    except Exception as e:
        print(f"  !! 严重错误: 处理文件 {file_name} 时失败: {e}")

# -----------------------------------------------------------------
# 3. 【最终合并 统计文件】(内存安全)
# -----------------------------------------------------------------
if not temp_stat_files:
    print("!! 严重错误: 未能处理任何批次，没有临时统计文件可合并。")
else:
    print(f"\n...所有批次的频率计算完毕。")
    print(f"开始合并 {len(temp_stat_files)} 个临时统计文件...")

    try:
        final_df_list = [pd.read_csv(f, engine='python', encoding='latin-1') for f in temp_stat_files]
        final_data = pd.concat(final_df_list, ignore_index=True)
        final_data = final_data.sort_values(by='Grid_ID')

        # 3. 最终输出文件
        output_file_name = f"{scenario_name}_FREQUENCY_STATS.csv"
        output_file_path = base_dir / output_file_name
        final_data.to_csv(output_file_path, index=False)

        print(f"\n====================================================")
        print(f"--- 成功！情景 {scenario_name} 的最终频率文件已保存到: {output_file_path} ---")
        print("====================================================")

        print("正在清理 TEMP_STATS 文件夹...")
        for f in temp_stat_files:
            os.remove(f)
        os.rmdir(temp_stats_dir)
        print("清理完毕。")

    except Exception as e:
        print(f"!! 严重错误: 在最终合并时失败: {e}")