import streamlit as st
import numpy as np
import pandas as pd
import plotly.express as px
from PIL import Image, ImageDraw
from pandas.api.types import (
    is_categorical_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype
)
import glob

# Define custom color scale with a distinct midpoint color
color_scale = [
    [0.0, 'rgb(255, 0, 0)'],
    [0.4, 'rgb(255, 255, 0)'],
    [0.5, 'rgb(255, 255, 255)'],
    [0.6, 'rgb(0, 255, 255)'],
    [1.0, 'rgb(0, 0, 255)']
]

reversed_color_scale = list(reversed(color_scale))

st.set_page_config(page_title='Weather data Visualization')
st.header('Weather data visualization')

# read csv file
#csv_file = 'combined_csv.csv'

#read csv file
csv_files = glob.glob('/usr/local/hadoop_namenode/*.csv')

dfs = []
for csv_file in csv_files:
    df = pd.read_csv(csv_file, usecols=lambda x: x != 'Unnamed: 0')
    dfs.append(df)

df = pd.concat(dfs, ignore_index=True)
#df = pd.read_csv(csv_file, usecols=lambda x: x != 'Unnamed: 0')
# Function to Filter columns in CSV

def filter_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    modify = st.checkbox("Add filters")

    if not modify:
        return df

    df = df.copy()

    # Try to convert datetimes into a standard format (datetime, no timezone)
    for col in df.columns:
        if is_object_dtype(df[col]):
            try:
                df[col] = pd.to_datetime(df[col])
            except Exception:
                pass

        if is_datetime64_any_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)

    modification_container = st.container()

    with modification_container:
        to_filter_columns = st.multiselect("Filter dataframe on", df.columns)
        for column in to_filter_columns:
            left, right = st.columns((1, 20))
            # Treat columns with < 10 unique values as categorical
            if is_categorical_dtype(df[column]) or df[column].nunique() < 10:
                user_cat_input = right.multiselect(
                    f"Values for {column}",
                    df[column].unique(),
                    default=list(df[column].unique()),
                )
                df = df[df[column].isin(user_cat_input)]
            elif is_numeric_dtype(df[column]):
                _min = float(df[column].min())
                _max = float(df[column].max())
                step = (_max - _min) / 100
                user_num_input = right.slider(
                    f"Values for {column}",
                    min_value=_min,
                    max_value=_max,
                    value=(_min, _max),
                    step=step,
                )
                df = df[df[column].between(*user_num_input)]
            elif is_datetime64_any_dtype(df[column]):
                user_date_input = right.date_input(
                    f"Values for {column}",
                    value=(
                        df[column].min(),
                        df[column].max(),
                    ),
                )
                if len(user_date_input) == 2:
                    user_date_input = tuple(
                        map(pd.to_datetime, user_date_input))
                    start_date, end_date = user_date_input
                    df = df.loc[df[column].between(start_date, end_date)]
            else:
                user_text_input = right.text_input(
                    f"Substring or regex in {column}",
                )
                if user_text_input:
                    df = df[df[column].astype(
                        str).str.contains(user_text_input)]

        filtered_df = df

    return filtered_df if 'filtered_df' in locals() else df

# Start of functions for Visualization Plots


new_col = []
for col in df.columns:
    new_col.append(col)
all_cols = tuple(new_col)
numeric_cols = tuple(df.select_dtypes(include=['float64', 'int64']))
datetime_cols = tuple(df.select_dtypes(include=['datetime64']))
string_cols = tuple(df.select_dtypes(include=['object']))
string_cols_limited = tuple(df.select_dtypes(
    include=['object']).loc[:, df.select_dtypes(include=['object']).nunique() < 20])
categorical_cols = tuple(df.select_dtypes(include=['object', 'category']))
heatmap_cols = []
for col in numeric_cols:
    if len(df[col].unique()) > 5:
        heatmap_cols.append(col)
heatmap_cols = tuple(heatmap_cols)


def piechart_cols(df):
    new_cols = []
    for col in df.columns:
        if is_categorical_dtype(df[col]) or df[col].nunique() < 10:
            new_cols.append(col)
    return tuple(new_cols)

# End of functions for visualization plots

# Function to Let user choose which plot to display


def visualization(plot_selected, df):
    # BOXPLOT
    if plot_selected == 'BoxPlot':
        st.write(
            'Used with numerical data, and if required, you can choose the category to compare boxplots with')
        option = st.selectbox(
            'Which column boxplot would you like to observe', numeric_cols)
        add_cat = st.checkbox("Add categories")
        if add_cat:
            option2 = st.selectbox(
                'Which column boxplot would you like to use as the categories', string_cols_limited)
            fig = px.box(df, y=option, color=option2,
                         color_discrete_sequence=["#636EFA"])
            fig.update_layout(
                title="Boxplot",
                yaxis_title=option,
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                ),
                xaxis=dict(
                    tickangle=-45,
                    title="",
                    tickfont=dict(size=14)
                ),
                yaxis=dict(
                    tickfont=dict(size=14)
                )
            )
            st.plotly_chart(fig)
        else:
            fig = px.box(df, y=option, color_discrete_sequence=["#636EFA"])
            fig.update_layout(
                title="Boxplot",
                yaxis_title=option,
                font=dict(
                    family="Courier New, monospace",
                    size=18,
                    color="#7f7f7f"
                ),
                xaxis=dict(
                    tickangle=-45,
                    title="",
                    tickfont=dict(size=14)
                ),
                yaxis=dict(
                    tickfont=dict(size=14)
                )
            )
            st.plotly_chart(fig)
    # PIECHART
    elif plot_selected == 'PieChart':
        st.write('Best used with categorical or nominal data')
        option = st.selectbox(
            'Which column piechart would you like to observe', piechart_cols(df))
        new_df = df.groupby([option])[
            option].count().reset_index(name='count')
        st.dataframe(new_df)
        fig = px.pie(new_df, values='count', names=option, title=option)
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(title='Distribution of Weather Conditions')
        st.plotly_chart(fig)
    # SCATTERPLOT
    elif plot_selected == 'ScatterPlot':
        st.write('Best used with 2 numeric variables')
        x_col = st.selectbox('Select x-axis column:', numeric_cols)
        y_col = st.selectbox('Select y-axis column:', numeric_cols)
        add_cat = st.checkbox("Add category to seperate points by")
        if add_cat:
            option1 = st.selectbox(
                'Which column would you like to use as the category seperator', string_cols_limited)
            fig = px.scatter(df, x=x_col, y=y_col,
                             hover_name=option1, color=option1)
        else:
            fig = px.scatter(df, x=x_col, y=y_col)
        fig.update_traces(opacity=0.7)
        title = 'Scatter Plot of ' + x_col + ' and ' + y_col
        fig.update_layout(
            title=title,
            xaxis_title=x_col,
            yaxis_title=y_col
        )
        st.plotly_chart(fig)
    # LINEPLOT
    elif plot_selected == 'LinePlot':
        st.write('Steps to set timeseries if available:')
        st.write('1) Set x-axis column as the datetime column')
        st.write('2) Set y-axis column as column to be observed')
        st.write('3) Use dataframe filter above as required')
        x_col = st.selectbox('Select x-axis column:', all_cols)
        y_col = st.selectbox('Select y-axis column:', numeric_cols)
        add_cat = st.checkbox("Add location to seperate lines by")
        if add_cat:
            option1 = st.selectbox(
                'Which column would you like to use as the location seperator', string_cols_limited)
            fig = px.line(df, x=x_col, y=y_col, color=option1)
        else:
            fig = px.line(df, x=x_col, y=y_col)
        fig.update_traces(opacity=0.7)
        fig.update_layout(
            title='Line Plot',
            xaxis_title=x_col,
            yaxis_title=y_col
        )
        st.plotly_chart(fig)
    # BARCHART
    elif plot_selected == 'BarChart':
        st.write('Best used with categorical or nominal data')
        x_col = st.selectbox('Select x-axis column:', all_cols)
        y_col = f"Count of {x_col}"
        fig = px.histogram(df, x=x_col)
        fig.update_layout(bargap=0.1)
        fig.update_traces(marker_color='#008080', opacity=0.7, marker_line_color='rgb(8,48,107)',
                          marker_line_width=1.5)
        st.plotly_chart(fig)
    # VIOLINCHART
    elif plot_selected == 'ViolinChart':
        x_col = st.selectbox('Select x-axis column:', categorical_cols)
        y_col = st.selectbox('Select y-axis column:', numeric_cols)
        fig = px.violin(df, x=x_col, y=y_col)
        fig.update_traces(opacity=0.7)
        fig.update_layout(
            title="Violin Chart",
            font=dict(size=14)
        )
        st.plotly_chart(fig)
    # HEATMAP
    elif plot_selected == 'HeatMap':
        correl_check = st.checkbox("HeatMap of correlation")
        if correl_check:
            fig = px.imshow(df.corr(),
                            color_continuous_scale='viridis',
                            labels=dict(x="Columns", y="Columns",
                                        color="Correlation"),
                            x=df.corr().columns,
                            y=df.corr().columns,
                            title="Correlation Heatmap")
            fig.update_layout(font=dict(family="Arial", size=12))
            st.plotly_chart(fig)
        else:
            try:
                x_col = st.selectbox('Select x-axis column:', heatmap_cols)
                y_col = st.selectbox('Select y-axis column:', heatmap_cols)
                matrix = pd.pivot_table(
                    df, values=y_col, index=x_col, aggfunc=np.mean)
                fig = px.imshow(matrix, color_continuous_scale='viridis',
                                title="Heatmap")
                fig.update_layout(font=dict(family="Arial", size=12))
                st.plotly_chart(fig)
            except:
                st.write('Error: X and Y axis must be different')
    # Scatter3D
    elif plot_selected == 'Scatter3d':
        x_col = st.selectbox('Select x-axis column:', numeric_cols)
        y_col = st.selectbox('Select y-axis column:', numeric_cols)
        z_col = st.selectbox('Select z-axis column:', numeric_cols)
        add_cat = st.checkbox("Add category feature to plot")
        if add_cat:
            option1 = st.selectbox(
                'Which column would you like to use as the category seperator', string_cols_limited)
            fig = px.scatter_3d(df, x=x_col, y=y_col,
                                z=z_col, color=option1)
        else:
            fig = px.scatter_3d(df, x=x_col, y=y_col, z=z_col)
        fig.update_traces(opacity=0.7, marker=dict(size=2))
        fig.update_layout(legend=dict(title='Scatter3D Plot',
                          orientation='h'), scene_camera=dict(eye=dict(x=2, y=2, z=2)))
        st.plotly_chart(fig, use_container_width=True,
                        height=2000, width=2000)
    # BUBBLECHART
    elif plot_selected == 'BubbleChart':
        try:
            x_col = st.selectbox('Select x-axis column:', numeric_cols)
            y_col = st.selectbox('Select y-axis column:', numeric_cols)
            size_col = st.selectbox('Select size column:', all_cols)
            fig = px.scatter(df, x=x_col, y=y_col,
                             size=size_col, size_max=30, opacity=0.7, color_continuous_scale=px.colors.sequential.Viridis, width=800, height=500)

            st.plotly_chart(fig)
        except:
            st.write('Invalid column set')
    # POLARCHART
    elif plot_selected == 'PolarChart':
        r = st.selectbox('Select radius column:', numeric_cols)
        theta = st.selectbox('Select theta column:', string_cols_limited)
        fig = px.line_polar(df, r=r, theta=theta,
                            line_close=True, title="PolarChart", width=600, height=600)
        st.plotly_chart(fig)
    # FUNNELCHART
    elif plot_selected == 'FunnelChart':
        st.write('Numeric data suitable for x')
        x_col = st.selectbox('Select x-axis column:', numeric_cols)
        st.write('Categoric data suitable for y')
        y_col = st.selectbox('Select y-axis column:', categorical_cols)
        fig = px.funnel(df, x=x_col, y=y_col)
        fig.update_layout(title="Funnel Chart",
                          xaxis_title=x_col, yaxis_title=y_col, margin=dict(l=50, r=50, t=50, b=50), funnelmode="overlay")
        st.plotly_chart(fig)
    # CHLORPLETH MAP
    elif plot_selected == 'Choropleth':
        geo_col = st.selectbox(
            'Select geo-location column:', string_cols)
        value_col = st.selectbox('Select value column:', numeric_cols)
        add_cat = st.checkbox("Add datetime feature to animate")
        if add_cat:
            option1 = st.selectbox(
                'Select datetime column', all_cols)
            fig = px.choropleth(df, locations=geo_col, locationmode='country names', color=value_col, animation_frame=option1,
                                hover_name=geo_col, projection='natural earth', color_continuous_scale=reversed_color_scale, title="Chloropleth Map")
        else:
            fig = px.choropleth(df, locations=geo_col, locationmode='country names', color=value_col,
                                hover_name=geo_col, projection='natural earth', color_continuous_scale=reversed_color_scale, title="Chloropleth Map")
        fig.update_layout(coloraxis=dict(colorscale="Viridis"))
        fig.update_layout(
            title=dict(x=0.5),
            font=dict(size=12),
            margin=dict(l=50, r=50, t=50, b=50),
            plot_bgcolor="white"
        )
        st.plotly_chart(fig)
    # SCATTERGEO
    elif plot_selected == 'ScatterGeo':
        country_col = st.selectbox(
            'Select country column:', string_cols)
        value_col = st.selectbox('Select value column:', numeric_cols)
        add_cat = st.checkbox("Add datetime feature to animate")
        if add_cat:
            option1 = st.selectbox(
                'Select datetime column', all_cols)
            fig = px.scatter_geo(df, locations=country_col, locationmode='country names', color=value_col, animation_frame=option1,
                                 hover_name=country_col, projection='natural earth', color_continuous_scale=px.colors.sequential.Plasma, title="ScatterGeo Map")
        else:
            fig = px.scatter_geo(df, locationmode='country names', hover_name=country_col,
                                 locations=country_col, color=value_col, title="ScatterGeo Map", projection='natural earth', color_continuous_scale=px.colors.sequential.Plasma)
        st.plotly_chart(fig)


filtered_df = filter_dataframe(df)
st.dataframe(filtered_df)

# let user select column to analyze in boxplot
plots = ['BoxPlot', 'PieChart', 'ScatterPlot',
         'LinePlot', 'BarChart', 'ViolinChart', 'HeatMap', 'Scatter3d', 'BubbleChart', 'PolarChart', 'FunnelChart', 'Choropleth', 'ScatterGeo']

plot_selected = st.selectbox(
    'Which plot would you like to visualize?', plots)

visualization(plot_selected, filtered_df)
