import pandas as pd
import plotly.express as px
from pathlib import Path
import os
import geopandas as gpd
from shapely.geometry import Point
import numpy as np

# -----------------------------------------------------------------
# 1. 【设置】(路径大集合)
# -----------------------------------------------------------------

# 情景 1: 1901soc (E盘)
dir_1901 = Path("E:/dissertation/countclim-1901soc")
file_1901 = dir_1901 / "countclim-1901soc_RATIO_STATS.csv"

# 情景 2: histsoc (E盘)
dir_hist = Path("E:/dissertation/countclim-histsoc")
file_hist = dir_hist / "countclim-histsoc_RATIO_STATS.csv"

# 情景 3: obsclim (F盘, 也是我们的"坐标主文件")
dir_obs = Path("F:/fyp/obsclim-histsoc")
file_obs = dir_obs / "obsclim-histsoc_RATIO_STATS.csv"

# Shapefile (假设在 E 盘 countclim-histsoc 里有)
shapefile_path = dir_hist / "1query_shape_copy.shp"

# 输出目录
output_dir = Path("E:/dissertation/FINAL_ATTRIBUTION_MAPS")
output_dir.mkdir(exist_ok=True, parents=True)

print("--- 正在生成最终归因地图 (Grand Finale) ---")

# -----------------------------------------------------------------
# 2. 读取数据 & 统一坐标
# -----------------------------------------------------------------
try:
    # 1. 读取主坐标文件 (Obs)
    df_obs = pd.read_csv(file_obs)
    master_coords = df_obs[['Grid_ID', 'Lon', 'Lat']].drop_duplicates()
    print("已读取主坐标源 (Obs).")

    # 2. 读取其他文件 (丢弃它们自带的坐标，防止缺口)
    df_1901 = pd.read_csv(file_1901).drop(columns=['Lon', 'Lat'], errors='ignore')
    df_hist = pd.read_csv(file_hist).drop(columns=['Lon', 'Lat'], errors='ignore')
    print("已读取 1901soc 和 histsoc 数据.")

    # 3. 合并数据
    # 合并 Hist
    df_merged = pd.merge(master_coords, df_hist, on='Grid_ID', how='left')
    df_merged = df_merged.rename(columns={
        'Drought_Ratio': 'DR_hist', 'Flood_Ratio': 'FR_hist'
    })

    # 合并 1901
    df_merged = pd.merge(df_merged, df_1901, on='Grid_ID', how='left')
    df_merged = df_merged.rename(columns={
        'Drought_Ratio': 'DR_1901', 'Flood_Ratio': 'FR_1901'
    })

    # 合并 Obs (为了计算 CC)
    # 注意：这里我们只取 Ratio 列，因为 Lon/Lat 已经在 master_coords 里了
    df_obs_ratios = df_obs[['Grid_ID', 'Drought_Ratio', 'Flood_Ratio']]
    df_merged = pd.merge(df_merged, df_obs_ratios, on='Grid_ID', how='left')
    df_merged = df_merged.rename(columns={
        'Drought_Ratio': 'DR_obs', 'Flood_Ratio': 'FR_obs'
    })

    print("数据合并与坐标统一完成。")

except Exception as e:
    print(f"!! 读取数据失败: {e}")
    exit()

# -----------------------------------------------------------------
# 3. 计算归因 (Delta)
# -----------------------------------------------------------------
print("正在计算归因指标...")

# 处理无穷大 (Inf)
# 如果 Ratio 是 Inf，我们把它设为一个较大的数字(比如5)以便计算差值
# 或者，您可以选择将差值设为 NaN。这里我们简单处理：
cols_to_fix = ['DR_hist', 'FR_hist', 'DR_1901', 'FR_1901', 'DR_obs', 'FR_obs']
for col in cols_to_fix:
    df_merged[col] = df_merged[col].replace([np.inf], 5.0)

# 1. 人类活动影响 (Delta HA = Hist - 1901)
df_merged['Delta_HA_Drought'] = df_merged['DR_hist'] - df_merged['DR_1901']
df_merged['Delta_HA_Flood'] = df_merged['FR_hist'] - df_merged['FR_1901']

# 2. 气候变化影响 (Delta CC = Obs - Hist)
df_merged['Delta_CC_Drought'] = df_merged['DR_obs'] - df_merged['DR_hist']
df_merged['Delta_CC_Flood'] = df_merged['FR_obs'] - df_merged['FR_hist']

# -----------------------------------------------------------------
# 4. 空间筛选 (只保留中国)
# -----------------------------------------------------------------
try:
    print("正在筛选中国区域...")
    china_boundary = gpd.read_file(shapefile_path).to_crs(epsg=4326)

    geometry = [Point(xy) for xy in zip(df_merged['Lon'], df_merged['Lat'])]
    gdf_points = gpd.GeoDataFrame(df_merged, crs="EPSG:4326", geometry=geometry)

    gdf_china = gpd.sjoin(gdf_points, china_boundary, how="inner", predicate='within')
    df_plot = pd.DataFrame(gdf_china.drop(columns=['geometry', 'index_right']))

    print(f"筛选完成。绘图点数: {len(df_plot)}")

except Exception as e:
    print(f"!! Shapefile 错误: {e}")
    exit()


# -----------------------------------------------------------------
# 5. 绘图函数
# -----------------------------------------------------------------
def plot_attribution_map(df, col_name, title, filename):
    # 自动调整颜色范围：以 0 为中心
    # 我们限制在 -1 到 1 之间，以便看清变化，超过这个范围的颜色会饱和

    fig = px.scatter(
        df,
        x='Lon',
        y='Lat',
        color=col_name,
        title=title,
        labels={col_name: 'Ratio Change (Diff)'},
        color_continuous_scale='RdBu_r',  # 红=增加, 蓝=减少
        color_continuous_midpoint=0,  # 关键：0 是白色/中性
        range_color=[-1, 1],  # 范围
        width=1000, height=800
    )
    fig.update_yaxes(scaleanchor="x", scaleratio=1)
    fig.update_layout(template='plotly_white')

    out_path = output_dir / filename
    fig.write_html(out_path)
    print(f"  -> 已保存: {filename}")


# -----------------------------------------------------------------
# 6. 生成 4 张最终地图
# -----------------------------------------------------------------

# 1. 人类活动 -> 干旱
plot_attribution_map(
    df_plot, 'Delta_HA_Drought',
    '人类活动对干旱频率比率的影响 (Delta HA)<br>(HistSoc - 1901Soc)',
    'Map_Attribution_HA_Drought.html'
)

# 2. 人类活动 -> 洪涝
plot_attribution_map(
    df_plot, 'Delta_HA_Flood',
    '人类活动对洪涝频率比率的影响 (Delta HA)<br>(HistSoc - 1901Soc)',
    'Map_Attribution_HA_Flood.html'
)

# 3. 气候变化 -> 干旱
plot_attribution_map(
    df_plot, 'Delta_CC_Drought',
    '气候变化对干旱频率比率的影响 (Delta CC)<br>(Obs - CountClim)',
    'Map_Attribution_CC_Drought.html'
)

# 4. 气候变化 -> 洪涝
plot_attribution_map(
    df_plot, 'Delta_CC_Flood',
    '气候变化对洪涝频率比率的影响 (Delta CC)<br>(Obs - CountClim)',
    'Map_Attribution_CC_Flood.html'
)

print("\n====================================================")
print("大功告成！所有归因地图已生成。")
print(f"请查看文件夹: {output_dir}")
print("====================================================")