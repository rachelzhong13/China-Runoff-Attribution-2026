import pandas as pd
import plotly.express as px
from pathlib import Path
import os
import geopandas as gpd
from shapely.geometry import Point

# -----------------------------------------------------------------
# 1. 【设置】
# (无需更改)
# -----------------------------------------------------------------

# 情景 1 (1901年固定社会)
scen_1901_path = Path("E:/dissertation/countclim-1901soc/countclim-1901soc_FREQUENCY_STATS.csv")

# 情景 2 (历史社会，无气候变暖)
scen_count_hist_path = Path("F:/fyp/countclim-histsoc/countclim-histsoc_FREQUENCY_STATS.csv")

# 情景 3 (观测)
scen_obs_path = Path("F:/fyp/obsclim-histsoc/obsclim-histsoc_FREQUENCY_STATS.csv")

# Shapefile 的名称
SHAPEFILE_NAME = "1query_shape_copy.shp"

# 归因分析的输出目录
output_dir = Path("E:/dissertation/ATTRIBUTION_RESULTS")
output_dir.mkdir(exist_ok=True)

# 要分析的列
frequency_columns = [
    "Drought_1.0", "Drought_1.5", "Flood_1.0", "Flood_1.5"
]

print("--- ------------------------------------------ ---")
print("--- 正在开始最终归因分析 (已集成 Geopandas 筛选) ---")
print("--- ------------------------------------------ ---")

# -----------------------------------------------------------------
# 2. 读取和合并数据
# (无需更改)
# -----------------------------------------------------------------
try:
    df_1901 = pd.read_csv(scen_1901_path)
    df_count_hist = pd.read_csv(scen_count_hist_path)
    df_obs = pd.read_csv(scen_obs_path)
    print("已成功读取所有 3 个情景的频率文件。")

    # 按 Grid_ID 合并
    df_1901_renamed = df_1901.add_suffix('_1901')
    df_count_hist_renamed = df_count_hist.add_suffix('_hist')
    df_obs_renamed = df_obs.add_suffix('_obs')

    df_merged = pd.merge(
        df_1901_renamed, df_count_hist_renamed,
        left_on='Grid_ID_1901', right_on='Grid_ID_hist'
    )
    df_merged = pd.merge(
        df_merged, df_obs_renamed,
        left_on='Grid_ID_1901', right_on='Grid_ID_obs'
    )

    # 清理并保留一组 Lon/Lat/Grid_ID
    df_final = df_merged.rename(columns={
        'Grid_ID_1901': 'Grid_ID', 'Lon_1901': 'Lon', 'Lat_1901': 'Lat'
    })
    cols_to_drop = [col for col in df_final.columns if
                    col.startswith(('Grid_ID_', 'Lon_', 'Lat_')) and col not in ['Grid_ID', 'Lon', 'Lat']]
    df_final = df_final.drop(columns=cols_to_drop)

    print(f"数据合并完毕。总共 {len(df_final)} 个网格点。")

except Exception as e:
    print(f"!! 严重错误: 读取或合并文件时失败: {e}")
    exit()

# -----------------------------------------------------------------
# 3. 【新】: 使用 Geopandas 筛选中国区域
# -----------------------------------------------------------------
try:
    # 我们从情景1的文件夹加载 shapefile
    shapefile_path = scen_1901_path.parent / SHAPEFILE_NAME
    if not shapefile_path.exists():
        print(f"!! 严重错误: 未找到 Shapefile: {shapefile_path}")
        print(f"!! 请确保 '{SHAPEFILE_NAME}' (及其 .shx, .dbf) 位于 {scen_1901_path.parent} 中。")
        exit()

    print(f"正在读取中国边界: {shapefile_path} ...")
    china_boundary = gpd.read_file(shapefile_path)
    china_boundary = china_boundary.to_crs(epsg=4326)  # 确保坐标系一致

    # 1. 将合并后的 DataFrame 转换为 GeoDataFrame
    geometry = [Point(xy) for xy in zip(df_final['Lon'], df_final['Lat'])]
    gdf_points = gpd.GeoDataFrame(df_final, crs="EPSG:4326", geometry=geometry)

    # 2. 执行空间连接 (Spatial Join)
    print("正在执行空间筛选...")

    # 【!! 此处是唯一的更改 !!】
    # 'op=' 已更改为 'predicate='
    gdf_china_only = gpd.sjoin(gdf_points, china_boundary, how="inner", predicate='within')

    # 3. 转换回普通的 DataFrame
    df_china_final = pd.DataFrame(gdf_china_only.drop(columns=['geometry', 'index_right']))

    print(f"筛选完毕。总共 {len(df_final)} 个网格点，其中 {len(df_china_final)} 个位于中国境内。")

    if df_china_final.empty:
        print("!! 严重错误: 筛选后没有剩余任何数据。请检查您的 shapefile 和网格坐标。")
        exit()

except Exception as e:
    print(f"!! 严重错误: Geopandas 筛选失败: {e}")
    print("!! 请确保您已复制了 .shp, .shx, 和 .dbf 文件。")
    exit()

# -----------------------------------------------------------------
# 4. 计算差异 (Delta_HA 和 Delta_CC)
# (无需更改)
# -----------------------------------------------------------------
print("正在计算人类活动 (HA) 和气候变化 (CC) 的影响...")

for col in frequency_columns:
    col_1901 = f"{col}_1901"
    col_hist = f"{col}_hist"
    col_obs = f"{col}_obs"

    col_delta_HA = f"Delta_HA_{col}"  # 人类活动
    col_delta_CC = f"Delta_CC_{col}"  # 气候变化

    df_china_final[col_delta_HA] = df_china_final[col_hist] - df_china_final[col_1901]
    df_china_final[col_delta_CC] = df_china_final[col_obs] - df_china_final[col_hist]

master_file = output_dir / "FINAL_ATTRIBUTION_STATS_CHINA_ONLY.csv"
df_china_final.to_csv(master_file, index=False)
print(f"最终归因主文件 (仅中国) 已保存到: {master_file}")

# -----------------------------------------------------------------
# 5. 【宏观分析】: 区域总和 (仅中国)
# (无需更改)
# -----------------------------------------------------------------
print("\n--- 1. 宏观分析 (仅中国区域总和) ---")
print("气候变化 (CC) 和人类活动 (HA) 对 *中国区域* 事件总次数的 *净影响*:")

for col in frequency_columns:
    delta_ha_col = f"Delta_HA_{col}"
    delta_cc_col = f"Delta_CC_{col}"

    total_delta_HA = df_china_final[delta_ha_col].sum()
    total_delta_CC = df_china_final[delta_cc_col].sum()

    print(f"  --- {col} ---")
    print(f"     人类活动 (Delta_HA) 影响: {total_delta_HA} 次")
    print(f"     气候变化 (Delta_CC) 影响: {total_delta_CC} 次")

# -----------------------------------------------------------------
# 6. 【具体分析】: 绘制纬度剖面图 (仅中国)
# (无需更改)
# -----------------------------------------------------------------
print("\n--- 2. 具体分析 (中国区域纬度剖面) ---")
print("正在生成归因剖面图...")

plot_files_created = 0
delta_columns = [col for col in df_china_final.columns if col.startswith("Delta_")]

for delta_col_name in delta_columns:
    df_sorted = df_china_final.sort_values(by='Lat')

    fig = px.line(
        df_sorted,
        x='Lat',
        y=delta_col_name,
        title=f'{delta_col_name} 随纬度的变化 (仅中国)',
        labels={'Lat': 'Latitude', delta_col_name: f'频率变化 (次数)'}
    )
    fig.add_hline(y=0, line_dash="dash", line_color="grey")

    output_file = output_dir / f"FINAL_ATTRIBUTION_{delta_col_name}_PROFILE_CHINA_ONLY.html"
    fig.write_html(output_file)
    plot_files_created += 1

print(f"成功！已在 {output_dir} 中生成 {plot_files_created} 张最终归因剖面图。")
print("====================================================")