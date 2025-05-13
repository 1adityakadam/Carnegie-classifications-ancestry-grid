import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd
import pyarrow.parquet as pq
import s3fs
import json

# ========== INITIALIZATION ==========
fs = s3fs.S3FileSystem(anon=True, client_kwargs={'region_name': 'us-east-2'})

# Load metadata and mappings
def initialize_app():
    # Load unique names from partitioned data
    unique_names = pq.read_table(
        's3://cchie-vborden/updated_data_grouped/',
        filesystem=fs,
        columns=['current_name']
    ).to_pandas()['current_name'].unique()

    # Load merged institution mappings
    merged_maps = pq.read_table(
        's3://cchie-vborden/updated_data_grouped/merged_mappings.parquet',
        filesystem=fs
    ).to_pandas()

    return {
        'unique_names': unique_names,
        'merged_into_map': merged_maps.set_index('unit_id')['current_name'].to_dict(),
        'merged_from_map': merged_maps.set_index('unit_id')['inst_name'].to_dict(),
        'desired_order': [
            'Doctoral', "Master's", "Bachelor's", 'Associates',
            'SF: 2Yr', 'SF: 4Yr', 'Tribal/Oth', 'Not in'
        ]
    }

app_data = initialize_app()

# ========== DASH APP LAYOUT ==========
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    dcc.Dropdown(
        id='current-name-dropdown',
        options=[{'label': n, 'value': n} for n in app_data['unique_names']],
        placeholder="Select Institution"
    ),
    html.Div(style={'height': '20px'}),
    html.Div(id='merged-into-display'),
    html.Div(style={'height': '20px'}),
    html.Table(id='year-degree-label-table')
], style={
    "border": "1px solid #1E90FF",
    "borderRadius": "5px",
    "padding": "10px",
    "backgroundColor": "#F0F8FF",
    "margin": "40px auto"
})

# ========== HELPER FUNCTIONS ==========
def load_partition(name):
    """Load partition data for selected institution"""
    try:
        # Find which group contains this institution
        dataset = pq.ParquetDataset(
            's3://cchie-vborden/updated_data_grouped/',
            filesystem=fs,
            filters=[('current_name', '==', name)]
        )
        return dataset.read().to_pandas()
    except:
        return pd.DataFrame()

def get_institution_info(unit_id, is_merged_from=False):
    """Get institution info using precomputed mappings"""
    if is_merged_from:
        return app_data['merged_from_map'].get(unit_id)
    return app_data['merged_into_map'].get(unit_id)

# ========== MAIN CALLBACK ==========
@app.callback(
    [Output('year-degree-label-table', 'children'),
     Output('merged-into-display', 'children')],
    [Input('current-name-dropdown', 'value')]
)
def update_display(selected_name):
    if not selected_name:
        return [], []

    # Load partition data
    partition_data = load_partition(selected_name)
    if partition_data.empty:
        return [], []

    # Prepare degree labels
    years = sorted(partition_data['year'].unique())
    inst_name_by_year = partition_data.groupby('year')['inst_name'].first().to_dict()

    # ===== Table Construction =====
    table_header = html.Tr([html.Th('Year')] + [html.Th(year) for year in years])
    inst_name_row = html.Tr([html.Td('Institution Name')] + [
        html.Td(inst_name_by_year.get(year, '')) for year in years
    ])

    # ===== Merge Display =====
    merge_elements = []
    
    # Handle Merged Into
    merged_into_ids = partition_data['merged_into_id'].dropna().unique()
    for unit_id in merged_into_ids:
        name = get_institution_info(unit_id)
        display = html.Span(
            f"{name} (ID: {unit_id})" if name else f"ID: {unit_id}",
            style={'backgroundColor': '#e6f4ff' if name else '#ffe6e6'}
        )
        merge_elements.append(html.Div([
            html.Span("Merged Into: ", style={'fontWeight': 'bold'}), display
        ]))

    # Handle Merged From
    merged_from_ids = partition_data['merged_from_id'].dropna().unique()
    for unit_id in merged_from_ids:
        name = get_institution_info(unit_id, is_merged_from=True)
        display = html.Span(
            f"{name} (ID: {unit_id})" if name else f"ID: {unit_id}",
            style={'backgroundColor': '#e6ffe6' if name else '#ffe6e6'}
        )
        merge_elements.append(html.Div([
            html.Span("Merged From: ", style={'fontWeight': 'bold'}), display
        ]))

    # ===== Dynamic Table Rows =====
    sorted_df = partition_data.drop_duplicates(
        subset=['year', 'degree_label', 'class_status']
    ).sort_values('degree_id')
    
    sorted_df['degree_label'] = pd.Categorical(
        sorted_df['degree_label'],
        categories=app_data['desired_order'],
        ordered=True
    ).sort_values()

    year_degree_mapping = sorted_df.groupby('year')['degree_label'].unique().to_dict()
    max_rows = max(len(year_degree_mapping.get(year, [])) for year in years)

    table_rows = []
    for row_idx in range(max_rows):
        row_cells = [html.Td('           ')]  # Empty first column
        for year in years:
            labels = year_degree_mapping.get(year, [])
            if row_idx < len(labels):
                degree_label = labels[row_idx]
                cell_data = partition_data[
                    (partition_data['year'] == year) & 
                    (partition_data['degree_label'] == degree_label)
                ]
                unique_labels = cell_data.sort_values('const_cat_value')['class_status'].unique()
                
                cell_content = [
                    html.P(
                        label,
                        style={'backgroundColor': 'lightblue'} 
                        if not cell_data[cell_data['class_status'] == label].empty 
                        else {}
                    )
                    for label in unique_labels
                ]
                
                row_cells.append(html.Td(
                    html.Details([
                        html.Summary(degree_label),
                        html.Div(cell_content)
                    ]),
                    style={'verticalAlign': 'top', 'borderBottom': '2px solid #ddd'}
                ))
            else:
                row_cells.append(html.Td(''))
        
        table_rows.append(html.Tr(row_cells))

    return [table_header, inst_name_row] + table_rows, merge_elements

# ========== LINK HANDLING CALLBACK ==========
@app.callback(
    Output('current-name-dropdown', 'value'),
    [Input({'type': 'merge-link', 'unit_id': ALL, 'index': ALL}, 'n_clicks'),
     Input({'type': 'merged-from-link', 'unit_id': ALL, 'index': ALL}, 'n_clicks')],
    [State({'type': 'merge-link', 'unit_id': ALL, 'index': ALL}, 'data-value'),
     State({'type': 'merged-from-link', 'unit_id': ALL, 'index': ALL}, 'data-value')]
)
def handle_merge_clicks(merge_clicks, merge_from_clicks, merge_values, merge_from_values):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update
    
    trigger_id = ctx.triggered[0]['prop_id']
    if 'merge-link' in trigger_id:
        for i, clicks in enumerate(merge_clicks):
            if clicks and i < len(merge_values):
                return merge_values[i]
    elif 'merged-from-link' in trigger_id:
        for i, clicks in enumerate(merge_from_clicks):
            if clicks and i < len(merge_from_values):
                return merge_from_values[i]
    
    return dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)
