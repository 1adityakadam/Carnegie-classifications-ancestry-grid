import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd
import json

# Load data from s3 bucket (currently not functional)
# new_data = pd.read_parquet(
#     'https://cchie-vborden.s3.us-east-2.amazonaws.com/updated_data.parquet',
#     storage_options={"anon": True}
# )

#Load data
new_data = pd.read_parquet('updated_data.parquet')

# Define the desired order for degree_label
desired_order = [
    'Doctoral',
    "Master's",
    "Bachelor's",
    'Associates',
    'SF: 2Yr',
    'SF: 4Yr',
    'Tribal/Oth',
    'Not in'
]

def get_unique_labels_for_year_degree_label(year, degree_label, data_frame):
    filtered_df = data_frame[(data_frame['year'] == year) & (data_frame['degree_label'] == degree_label)]
    unique_labels = filtered_df.sort_values(by='const_cat_value')['class_status'].unique().tolist()
    return unique_labels

# Prepare data
new_df_with_unique_labels = new_data.drop_duplicates(subset=['year', 'degree_label', 'class_status'])
sorted_df = new_df_with_unique_labels.sort_values(by='degree_id')
sorted_df['degree_label'] = pd.Categorical(sorted_df['degree_label'], categories=desired_order, ordered=True)
sorted_df = sorted_df.sort_values('degree_label')
all_degree_labels_sorted = sorted_df['degree_label'].unique()
year_degree_label_mapping = new_df_with_unique_labels.groupby('year')['degree_label'].unique().to_dict()

app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    dcc.Dropdown(
        id='current-name-dropdown',
        options=[{'label': name, 'value': name} for name in new_data['current_name'].unique()],
        placeholder="Select a Current Name"
    ),
    html.Div(style={'height': '20px'}),
    html.Div(id='merged-into-display'),
    html.Div(style={'height': '20px'}),
    html.Table(id='year-degree-label-table'),
    ],
    style={
        "borderRadius": "5px",               
        "padding": "10px",                   
        "backgroundColor": "#F0F8FF",        
        "margin": "40px auto"                
    }
)

@app.callback(
    [Output('year-degree-label-table', 'children'),
     Output('merged-into-display', 'children')],
    [Input('current-name-dropdown', 'value')]
)
def update_table(selected_current_name):
    if not selected_current_name:
        return [], ""

    years = sorted(new_data['year'].unique())
    inst_name_by_year = new_data[new_data['current_name'] == selected_current_name].groupby('year')['inst_name'].first().to_dict()

    filtered_data = new_data[new_data['current_name'] == selected_current_name]
    merged_into_exists = 'merged_into_id' in filtered_data.columns and not filtered_data['merged_into_id'].isnull().all()

    table_header_cells = [html.Th("Year")] + [html.Th(year) for year in years]
    if merged_into_exists:
        table_header_cells.append(html.Th("Merged Into"))
    table_header = html.Tr(table_header_cells)

    inst_name_row_cells = [html.Th("Institution Name")] + [html.Th(inst_name_by_year.get(year, 'N/A')) for year in years]
    if merged_into_exists:
        merged_into_value = filtered_data['merged_into_id'].iloc[0]
        inst_name_row_cells.append(html.Th(merged_into_value))
    inst_name_row = html.Tr(inst_name_row_cells)

    display_elements = []
    # --- Merged Into ---
    if 'merged_into_id' in filtered_data.columns and not filtered_data['merged_into_id'].isnull().all():
        merged_into_value = filtered_data['merged_into_id'].dropna().unique()[0]
        associated_data = new_data[new_data['unit_id'] == merged_into_value]
        associated_names = [name for name in associated_data['current_name'].unique().tolist() if pd.notnull(name) and name != "None"]

        display_elements.append(html.Span("Merged Into: ", style={'font-weight': 'bold'}))
        display_elements.append(html.Span(f"{merged_into_value}", style={'background-color': 'lightblue', 'font-weight': 'bold'}))
        if associated_names:
            display_elements.append(html.Span(", Merged Into Name: ", style={'font-weight': 'bold'}))
            for i, name in enumerate(associated_names):
                # Make link ID unique by including merged_into_value
                display_elements.append(
                    html.A(
                        f"{name}", href="#",
                        id={'type': 'merge-link', 'unit_id': str(merged_into_value), 'index': i},
                        **{'data-value': name},
                        style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                    )
                )
                if i < len(associated_names) - 1:
                    display_elements.append(html.Span(", ", style={'font-weight': 'normal'}))

    # --- Merged From ---
    if 'unit_id' in filtered_data.columns:
        current_unit_id = filtered_data['unit_id'].iloc[0]
        merged_from_records = new_data[new_data['merged_into_id'] == current_unit_id]
        if not merged_from_records.empty:
            if display_elements:
                display_elements.append(html.Br())
                display_elements.append(html.Br())
            display_elements.append(html.Span("Merged From: ", style={'font-weight': 'bold'}))
            merged_from_info = merged_from_records[['unit_id', 'current_name']].drop_duplicates()
            for i, (idx, row) in enumerate(merged_from_info.iterrows()):
                unit_id = row['unit_id']
                display_elements.append(html.Span(f"{unit_id}", style={'background-color': 'lightblue', 'font-weight': 'bold'}))
                inst_names_data = new_data[new_data['unit_id'] == unit_id]
                if not inst_names_data.empty and 'inst_name' in inst_names_data.columns:
                    inst_names = [name for name in inst_names_data['inst_name'].unique() if pd.notnull(name) and name != "None"]
                    if len(inst_names) > 0:
                        display_elements.append(html.Span(", Merged From Names: ", style={'font-weight': 'bold'}))
                        for j, name in enumerate(inst_names):
                            # Make link ID unique by including unit_id
                            display_elements.append(
                                html.A(
                                    f"{name}", href="#",
                                    id={'type': 'merged-from-link', 'unit_id': str(unit_id), 'index': j},
                                    **{'data-value': row['current_name']},
                                    style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                                )
                            )
                            if j < len(inst_names) - 1:
                                display_elements.append(html.Span(", ", style={'font-weight': 'normal'}))
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

    # Always return a new Div for display_elements so old links are cleared
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
