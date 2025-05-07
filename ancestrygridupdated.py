from flask import jsonify
import dash
from dash import dcc, html
from dash.dependencies import Input, Output, State, ALL
import pandas as pd

# Load data
new_data = pd.read_parquet('new_updated_data.parquet')

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

# Initialize Dash app and expose server
app = dash.Dash(__name__)
server = app.server

app.layout = html.Div([
    dcc.Dropdown(
        id='current-name-dropdown',
        options=[{'label': name, 'value': name} for name in new_data['current_name'].unique()],
        placeholder="Select a Current Name"
    ),
    html.Div(id='merged-into-display'),
    html.Table(id='year-degree-label-table')
])

@app.server.route('/get_data')
def get_data():
    return jsonify(new_data.to_dict(orient="records"))

@app.callback(
    [Output('year-degree-label-table', 'children'),
     Output('merged-into-display', 'children')],
    [Input('current-name-dropdown', 'value')]
)
def update_table(selected_current_name):
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

    merged_into_display = []
    if selected_current_name:
        filtered_data = new_data[new_data['current_name'] == selected_current_name]
        if 'merged_into_id' in filtered_data.columns and not filtered_data['merged_into_id'].isnull().all():
            merged_into_value = filtered_data['merged_into_id'].dropna().unique()[0]
            associated_data = new_data[new_data['unit_id'] == merged_into_value]
            associated_names = associated_data['current_name'].unique().tolist()

            merged_into_display.append(html.Span("Merged Into: ", style={'font-weight': 'bold'}))
            merged_into_display.append(html.Span(f"{merged_into_value}", style={'background-color': 'yellow', 'font-weight': 'bold'}))
            if associated_names:
                merged_into_display.append(html.Span(", Associated Names: ", style={'font-weight': 'bold'}))
                for i, name in enumerate(associated_names):
                    merged_into_display.append(
                        html.A(
                            f"{name}", href="#", id={'type': 'merge-link', 'index': i},
                            **{'data-value': name},
                            style={'color': 'blue', 'font-weight': 'bold', 'cursor': 'pointer'}
                        )
                    )
                    merged_into_display.append(html.Span(", ", style={'font-weight': 'normal'}))
    if merged_into_display:
        merged_into_display = merged_into_display[:-1]  # Remove last comma

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
                        style={'background-color': 'yellow'} if label in new_data[
                            (new_data['current_name'] == selected_current_name) &
                            (new_data['year'] == year)
                        ]['class_status'].values else {}
                    )
                    for label in labels
                ]
                summary_style = {'background-color': '#FFD700', 'border-bottom': '1px solid black'} \
                    if any(label.style and label.style.get('background-color') == 'yellow' for label in cell_content) else {}
                columns_content[year].append(
                    html.Details(
                        [html.Summary(degree_label, style=summary_style), html.Div(cell_content)],
                        open=any(label.style and label.style.get('background-color') == 'yellow' for label in cell_content)
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

    return [table_header, inst_name_row] + table_rows, html.Div(merged_into_display)

@app.callback(
    Output('current-name-dropdown', 'value'),
    [Input({'type': 'merge-link', 'index': ALL}, 'n_clicks')],
    [State({'type': 'merge-link', 'index': ALL}, 'data-value')]
)
def update_dropdown_on_click(n_clicks_list, data_values):
    if n_clicks_list is not None and any(n_clicks_list):
        clicked_index = n_clicks_list.index(max(n_clicks_list))
        return data_values[clicked_index]
    return dash.no_update

if __name__ == '__main__':
    app.run_server()
