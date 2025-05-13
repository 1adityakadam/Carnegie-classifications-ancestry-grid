import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd
import pyarrow.parquet as pq
import s3fs

# --- S3 and Partition Setup ---
fs = s3fs.S3FileSystem(anon=True, client_kwargs={'region_name': 'us-east-2'})

# Load unique names and mapping from partitioned data
unique_names_df = pq.read_table(
    's3://cchie-vborden/updated_data_grouped/',
    filesystem=fs,
    columns=['current_name']
).to_pandas()
unique_names = sorted(unique_names_df['current_name'].unique())
name_to_group = {name: idx // 10 for idx, name in enumerate(unique_names)}

# Load merged institution mappings (precomputed)
merged_maps = pq.read_table(
    's3://cchie-vborden/updated_data_grouped/merged_mappings.parquet',
    filesystem=fs
).to_pandas()
merged_into_map = merged_maps.set_index('unit_id')['current_name'].to_dict()
merged_from_map = merged_maps.set_index('unit_id')['inst_name'].to_dict()

# --- Degree label order ---
desired_order = [
    'Doctoral', "Master's", "Bachelor's", 'Associates',
    'SF: 2Yr', 'SF: 4Yr', 'Tribal/Oth', 'Not in'
]

# --- Helper Functions ---
def load_partition(selected_name):
    group_id = name_to_group.get(selected_name)
    if group_id is None:
        return pd.DataFrame()
    try:
        return pq.read_table(
            f's3://cchie-vborden/updated_data_grouped/group_id={group_id}',
            filters=[('current_name', '==', selected_name)],
            filesystem=fs
        ).to_pandas()
    except Exception:
        return pd.DataFrame()

def get_unique_labels_for_year_degree_label(year, degree_label, data_frame):
    filtered_df = data_frame[(data_frame['year'] == year) & (data_frame['degree_label'] == degree_label)]
    unique_labels = filtered_df.sort_values(by='const_cat_value')['class_status'].unique().tolist()
    return unique_labels

# --- Dash App ---
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    dcc.Dropdown(
        id='current-name-dropdown',
        options=[{'label': name, 'value': name} for name in unique_names],
        placeholder="Select a Current Name"
    ),
    html.Div(style={'height': '20px'}),
    html.Div(id='merged-into-display'),
    html.Div(style={'height': '20px'}),
    html.Table(id='year-degree-label-table'),
], style={
    "border": "1px solid #1E90FF",
    "borderRadius": "5px",
    "padding": "10px",
    "backgroundColor": "#F0F8FF",
    "margin": "40px auto"
})

@app.callback(
    [Output('year-degree-label-table', 'children'),
     Output('merged-into-display', 'children')],
    [Input('current-name-dropdown', 'value')]
)
def update_table(selected_current_name):
    if not selected_current_name:
        return [], ""

    # Load only relevant partition
    new_data = load_partition(selected_current_name)
    if new_data.empty:
        return [], ""

    # Prepare year and institution name mapping
    years = sorted(new_data['year'].unique())
    inst_name_by_year = new_data.groupby('year')['inst_name'].first().to_dict()

    # Prepare degree label mappings
    new_df_with_unique_labels = new_data.drop_duplicates(subset=['year', 'degree_label', 'class_status'])
    sorted_df = new_df_with_unique_labels.sort_values(by='degree_id')
    sorted_df['degree_label'] = pd.Categorical(sorted_df['degree_label'], categories=desired_order, ordered=True)
    sorted_df = sorted_df.sort_values('degree_label')
    all_degree_labels_sorted = sorted_df['degree_label'].unique()
    year_degree_label_mapping = new_df_with_unique_labels.groupby('year')['degree_label'].unique().to_dict()

    # --- Table header ---
    merged_into_exists = 'merged_into_id' in new_data.columns and not new_data['merged_into_id'].isnull().all()
    table_header_cells = [html.Th("Year")] + [html.Th(year) for year in years]
    if merged_into_exists:
        table_header_cells.append(html.Th("Merged Into"))
    table_header = html.Tr(table_header_cells)

    # --- Institution name row ---
    inst_name_row_cells = [html.Th("Institution Name")] + [html.Th(inst_name_by_year.get(year, 'N/A')) for year in years]
    if merged_into_exists:
        merged_into_value = new_data['merged_into_id'].iloc[0]
        inst_name_row_cells.append(html.Th(merged_into_value))
    inst_name_row = html.Tr(inst_name_row_cells)

    # --- Merged Into and Merged From Display ---
    display_elements = []

    # --- Merged Into ---
    if 'merged_into_id' in new_data.columns and not new_data['merged_into_id'].isnull().all():
        merged_into_value = new_data['merged_into_id'].dropna().unique()[0]
        merged_into_name = merged_into_map.get(merged_into_value)
        display_elements.append(html.Span("Merged Into: ", style={'font-weight': 'bold'}))
        display_elements.append(html.Span(f"{merged_into_value}", style={'background-color': 'lightblue', 'font-weight': 'bold'}))
        if merged_into_name:
            display_elements.append(html.Span(", Merged Into Name: ", style={'font-weight': 'bold'}))
            display_elements.append(
                html.A(
                    f"{merged_into_name}", href="#",
                    id={'type': 'merge-link', 'unit_id': str(merged_into_value), 'index': 0},
                    **{'data-value': merged_into_name},
                    style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                )
            )

    # --- Merged From ---
    if 'unit_id' in new_data.columns:
        current_unit_id = new_data['unit_id'].iloc[0]
        # Merged from: find all institutions for which this is the merged_into_id
        # We need to scan all mappings for this
        merged_from_records = merged_maps[merged_maps['merged_into_id'] == current_unit_id] if 'merged_into_id' in merged_maps.columns else pd.DataFrame()
        if not merged_from_records.empty:
            if display_elements:
                display_elements.append(html.Br())
                display_elements.append(html.Br())
            display_elements.append(html.Span("Merged From: ", style={'font-weight': 'bold'}))
            merged_from_info = merged_from_records[['unit_id', 'inst_name']].drop_duplicates()
            for i, (idx, row) in enumerate(merged_from_info.iterrows()):
                unit_id = row['unit_id']
                inst_name = row['inst_name']
                display_elements.append(html.Span(f"{unit_id}", style={'background-color': 'lightblue', 'font-weight': 'bold'}))
                if inst_name and inst_name != "None":
                    display_elements.append(html.Span(", Merged From Names: ", style={'font-weight': 'bold'}))
                    display_elements.append(
                        html.A(
                            f"{inst_name}", href="#",
                            id={'type': 'merged-from-link', 'unit_id': str(unit_id), 'index': i},
                            **{'data-value': inst_name},
                            style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                        )
                    )
                if i < len(merged_from_info) - 1:
                    display_elements.append(html.Br())
                    display_elements.append(html.Br())

    # --- Table content ---
    table_rows = []
    max_rows = max(len(year_degree_label_mapping.get(year, [])) for year in years)
    columns_content = {year: [] for year in years}

    for degree_label in all_degree_labels_sorted:
        for year in years:
            if degree_label in year_degree_label_mapping.get(year, []):
                labels = get_unique_labels_for_year_degree_label(year, degree_label, new_df_with_unique_labels)
                cell_content = [
                    html.P(
                        label,
                        style={'background-color': 'lightblue'} if label in new_data[
                            (new_data['current_name'] == selected_current_name) &
                            (new_data['year'] == year)
                        ]['class_status'].values else {}
                    )
                    for label in labels
                ]
                summary_style = {'background-color': 'lightblue', 'border-bottom': '1px solid black'} \
                    if any(label.style and label.style.get('background-color') == 'lightblue' for label in cell_content) else {}
                columns_content[year].append(
                    html.Details(
                        [html.Summary(degree_label, style=summary_style), html.Div(cell_content)],
                        open=any(label.style and label.style.get('background-color') == 'lightblue' for label in cell_content)
                    )
                )
            else:
                columns_content[year].append('')

    for i in range(max_rows):
        row = [
            html.Td(
                columns_content[year][i] if i < len(columns_content[year]) else '',
                style={'vertical-align': 'top', 'border-bottom': '2px solid #ddd'}
            ) for year in years
        ]
        row.insert(0, html.Td('           '))
        table_rows.append(html.Tr(row))

    return [table_header, inst_name_row] + table_rows, html.Div(display_elements)

@app.callback(
    Output('current-name-dropdown', 'value'),
    [Input({'type': 'merge-link', 'unit_id': ALL, 'index': ALL}, 'n_clicks'),
     Input({'type': 'merged-from-link', 'unit_id': ALL, 'index': ALL}, 'n_clicks')],
    [State({'type': 'merge-link', 'unit_id': ALL, 'index': ALL}, 'data-value'),
     State({'type': 'merged-from-link', 'unit_id': ALL, 'index': ALL}, 'data-value')]
)
def update_dropdown_on_click(merge_into_clicks, merge_from_clicks, merge_into_values, merge_from_values):
    ctx = dash.callback_context
    if not ctx.triggered:
        return dash.no_update
    trigger_prop_id = ctx.triggered[0]['prop_id']
    # Handle clicks on "Merged Into" links
    if 'merge-link' in trigger_prop_id:
        for i, clicks in enumerate(merge_into_clicks):
            if clicks and i < len(merge_into_values):
                return merge_into_values[i]
    # Handle clicks on "Merged From" links
    elif 'merged-from-link' in trigger_prop_id:
        for i, clicks in enumerate(merge_from_clicks):
            if clicks and i < len(merge_from_values):
                return merge_from_values[i]
    return dash.no_update

if __name__ == '__main__':
    app.run_server(debug=True)
