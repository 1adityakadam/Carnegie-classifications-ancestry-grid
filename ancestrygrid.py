import os
import pandas as pd
import orjson
import psutil

from flask import Response
from flask_caching import Cache

import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL

# --- Configuration ---
DATA_FILE = 'new_data.parquet'
PREPROCESSED_FILE = 'preprocessed_data.parquet'
CACHE_TIMEOUT = 3600

# --- Data Preprocessing ---
def preprocess_data():
    if not os.path.exists(PREPROCESSED_FILE):
        print("Preprocessing data...")
        df = pd.read_parquet(DATA_FILE, engine='pyarrow')
        categorical_cols = ['GLabel', 'Label', 'SubCatLbl']
        df[categorical_cols] = df[categorical_cols].astype('category')
        desired_order = [
            'Doctoral', "Master's", "Bachelor's", 'Associates',
            'SF: 2Yr', 'SF: 4Yr', 'Tribal/Oth', 'Not in'
        ]
        df['GLabel'] = pd.Categorical(df['GLabel'], categories=desired_order, ordered=True)
        new_df = df.drop_duplicates(subset=['YEAR', 'GLabel', 'Label'])
        new_df = new_df.sort_values(by=['GLabel', 'SubCatLbl'])
        new_df.to_parquet(PREPROCESSED_FILE, engine='pyarrow')
        print("Preprocessing complete.")

preprocess_data()

# --- Load Data ---
def load_data():
    df = pd.read_parquet(PREPROCESSED_FILE, engine='pyarrow')
    return df

new_data = load_data()

# --- Helper Functions ---
def get_unique_labels_for_year_glabel(year, glabel, df):
    filtered = df[(df['YEAR'] == year) & (df['GLabel'] == glabel)]
    return filtered.sort_values('Label')['Label'].unique().tolist()

# --- Dash App Setup ---
app = dash.Dash(__name__)
server = app.server

cache = Cache(app.server, config={
    'CACHE_TYPE': 'simple',
    'CACHE_THRESHOLD': 100
})

desired_order = [
    'Doctoral', "Master's", "Bachelor's", 'Associates',
    'SF: 2Yr', 'SF: 4Yr', 'Tribal/Oth', 'Not in'
]
all_glabels_sorted = [g for g in desired_order if g in new_data['GLabel'].unique()]
years_sorted = sorted(new_data['YEAR'].unique())
most_recent_names = new_data['MostRecentName'].unique()

# --- Layout ---
app.layout = html.Div([
    html.H3("Institution Data Dashboard"),
    dcc.Dropdown(
        id='most-recent-name-dropdown',
        options=[{'label': name, 'value': name} for name in most_recent_names],
        placeholder="Select a MostRecentName"
    ),
    html.Div(id='link-unit-display'),
    html.Table(id='year-glabel-table'),
    html.Hr(),
    html.Div(id='perf-metrics', style={'fontSize': '12px', 'color': 'gray'}),
    dcc.Interval(id='interval-component', interval=10*1000, n_intervals=0)
])

# --- API Endpoint ---
@app.server.route('/get_data')
def get_data():
    return Response(
        orjson.dumps(new_data.to_dict(orient="records")),
        mimetype='application/json'
    )

# --- Caching ---
@cache.memoize(timeout=CACHE_TIMEOUT)
def get_filtered_data(selected_name):
    return new_data[new_data['MostRecentName'] == selected_name]

# --- Callbacks ---
@app.callback(
    [Output('year-glabel-table', 'children'),
     Output('link-unit-display', 'children')],
    [Input('most-recent-name-dropdown', 'value')],
    prevent_initial_call=True
)
def update_table(selected_name):
    if not selected_name:
        return [], ""
    filtered = get_filtered_data(selected_name)
    instnm_by_year = filtered.groupby('YEAR')['Instnm'].first().to_dict()

    link_unit_exists = 'LinkUnit' in filtered.columns and not filtered['LinkUnit'].isnull().all()
    table_header_cells = [html.Th("GLabel")] + [html.Th(year) for year in years_sorted]
    if link_unit_exists:
        table_header_cells.append(html.Th("LinkUnit"))
    table_header = html.Tr(table_header_cells)

    instnm_row_cells = [html.Th("Instnm")] + [html.Th(instnm_by_year.get(year, 'N/A')) for year in years_sorted]
    if link_unit_exists:
        link_unit_value = filtered['LinkUnit'].iloc[0]
        instnm_row_cells.append(html.Th(link_unit_value))
    instnm_row = html.Tr(instnm_row_cells)

    # LinkUnit display
    link_unit_display = []
    if link_unit_exists:
        link_unit_value = filtered['LinkUnit'].dropna().unique()[0]
        associated_data = new_data[new_data['UNITID'] == link_unit_value]
        associated_names = associated_data['MostRecentName'].unique().tolist()
        link_unit_display.append(html.Span("Merged Into: ", style={'font-weight': 'bold'}))
        link_unit_display.append(html.Span(f"{link_unit_value}", style={'background-color': 'yellow', 'font-weight': 'bold'}))
        if associated_names:
            link_unit_display.append(html.Span(", Associated Names: ", style={'font-weight': 'bold'}))
            for i, name in enumerate(associated_names):
                link_unit_display.append(
                    html.A(
                        f"{name}", href="#", id={'type': 'merge-link', 'index': i},
                        **{'data-value': name},
                        style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                    )
                )
                link_unit_display.append(html.Span(", ", style={'font-weight': 'normal'}))
        if link_unit_display:
            link_unit_display = link_unit_display[:-1]  # Remove trailing comma

    # Table rows
    year_glabel_mapping = new_data.groupby('YEAR')['GLabel'].unique().to_dict()
    max_rows = max(len(year_glabel_mapping.get(year, [])) for year in years_sorted)
    table_rows = []

    for glabel in all_glabels_sorted:
        row = [html.Td(glabel, style={'font-weight':'bold'})]
        for year in years_sorted:
            if glabel in year_glabel_mapping.get(year, []):
                labels = get_unique_labels_for_year_glabel(year, glabel, new_data)
                cell_content = [
                    html.P(
                        label,
                        style={'background-color': 'yellow'} if label in filtered[filtered['YEAR'] == year]['Label'].values else {}
                    )
                    for label in labels
                ]
                row.append(html.Td(cell_content, style={'vertical-align': 'top', 'border-bottom': '2px solid #ddd'}))
            else:
                row.append(html.Td(''))
        if link_unit_exists:
            row.append(html.Td(filtered['LinkUnit'].iloc[0]))
        table_rows.append(html.Tr(row))

    return [table_header, instnm_row] + table_rows, html.Div(link_unit_display)

@app.callback(
    Output('most-recent-name-dropdown', 'value'),
    [Input({'type': 'merge-link', 'index': ALL}, 'n_clicks')],
    [State({'type': 'merge-link', 'index': ALL}, 'data-value')]
)
def update_dropdown_on_click(n_clicks_list, data_values):
    if n_clicks_list and any(n_clicks_list):
        clicked_index = n_clicks_list.index(max(n_clicks_list))
        return data_values[clicked_index]
    return dash.no_update

@app.callback(
    Output('perf-metrics', 'children'),
    Input('interval-component', 'n_intervals')
)
def update_metrics(n):
    mem = psutil.virtual_memory()
    return f"Memory usage: {mem.percent}% | Available: {mem.available/1e9:.1f} GB"

# --- Run ---
if __name__ == '__main__':
    app.run_server(host="0.0.0.0", port=8050)
